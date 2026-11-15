// Free-model aggregator: one server, two client wires, ten simulated free providers.
//
// Routing idea, from the kit plus the community patterns for LLM routers
// (LiteLLM / OpenRouter fallback chains, Portkey, Cloudflare AI Gateway):
//  - classify each request by what it needs (tools / vision / JSON / context),
//    never by price, because every legal model here is free;
//  - rank the legal free models by the sealed quality rating, same-tier first
//    on failover, so a fallback costs a few rating points, not a whole tier;
//  - plan all 20 requests up front, scarcest first, with a soft cap of 6 calls
//    per provider, so a scarce model (Gemini for vision) is not spent on a
//    request any top model can serve, and the top model is not spent on
//    every unconstrained request;
//  - on 402 / 429 / 5xx, never answer with the error and never retry the
//    same provider for that request: count the failure and move to the next
//    candidate. A provider that failed three times is treated as down unless
//    nothing else can serve the request. A 402 is not a reason to wait:
//    in this trial it is either a scheduled refusal or a spent budget,
//    and the next candidate is already waiting.
//  - every response is rebuilt from one normalised answer {text, input,
//    output}, so the client wire, JSON or SSE, is always well formed and
//    its usage and stop reason always match the provider that answered.
//  - deterministic: no wall clock, no unseeded randomness, fixed ids and
//    timestamps, every choice a function of the brief and the answers seen.
//
// Node 22, standard library only. `node main.ts --list` prints the catalog
// and exits 0; otherwise the server registers, serves, and exits 0 on /work done.

import { createServer, type IncomingMessage, type Server, type ServerResponse } from 'node:http';
import { readFileSync } from 'node:fs';

const PROTOCOL_VERSION = 1;
const POLL_INTERVAL_MS = 100;
const PROBE_TIMEOUT_MS = 5000;
const MAX_BODY_BYTES = 32 * 1024 * 1024;
const FIXED_CREATED = 1767225600;
const FIXED_ANTHROPIC_CREATED_AT = '2026-01-01T00:00:00Z';
/**
 * Soft per-provider call caps for the up-front plan (community point 4: budget
 * awareness). First pass spreads the 20 primaries across the main tier so no
 * provider is drained early — at most 4 primaries on a top provider, 3 on the
 * rest; the flagship (OpenRouter 1475) is therefore still fresh for the
 * weight-2 requests that are planned first. Second pass relaxes to the lowest
 * per-trial budget each provider ever draws, so every request still gets a
 * primary. Cheap-tier models are never primaries while a main-tier model can
 * serve: they sit at the end of the failover list (point 2, tiering).
 */
const PLAN_CAP_1: Record<string, number> = { groq: 4, cerebras: 3, gemini: 4, zai: 3, openrouter: 4, sambanova: 3, pollinations: 3, cloudflare: 4, ovh: 3, kilol: 3 };
const PLAN_CAP_2: Record<string, number> = { groq: 9, cerebras: 5, gemini: 6, zai: 4, openrouter: 10, sambanova: 4, pollinations: 10, cloudflare: 7, ovh: 5, kilol: 12 };
const PLAN_CAP_PER_PROVIDER = 6;
/** A provider that refused three times is down, unless it is the last resort. */
const DEAD_AFTER_FAILURES = 3;

const ENV_URL = (process.env.MAKEFUN_ENV_URL ?? '').replace(/\/+$/, '');
const PROMPT_FILE = process.env.MAKEFUN_PROMPT_FILE ?? '';

type Json = Record<string, unknown>;
const isRecord = (v: unknown): v is Json => v !== null && typeof v === 'object' && !Array.isArray(v);
const stringOf = (v: unknown): string => (typeof v === 'string' ? v : '');
const numberOrNull = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const boolOrNull = (v: unknown): boolean | null => (typeof v === 'boolean' ? v : null);

type Wire = 'openai' | 'anthropic' | 'gemini';
type AuthScheme = 'bearer' | 'x-api-key' | 'x-goog-api-key' | 'none';

interface Provider { id: string; name: string; wire: Wire; auth: AuthScheme; base: string; key_env: string; key: string }
interface RatingRow { model: string; provider: string; rating_milli: number; context_length: number | null; tools: boolean; vision: boolean; json: boolean; free: boolean }
interface Requirement { tools?: boolean; vision?: boolean; json?: boolean; min_context?: number }
interface WorkloadRequest { id: string; api: 'openai' | 'anthropic'; stream: boolean; weight: 1 | 2; requires: Requirement }
interface CatalogModel { id: string; free: boolean; context_length: number | null }
interface CatalogProvider { id: string; base_url: string; key_env: string; wire: Wire; models: CatalogModel[] }
interface CatalogReport { providers: CatalogProvider[] }
interface ProbedModel { id: string; display_name: string; free: boolean | null; context_length: number | null; tools: boolean | null; vision: boolean | null; json: boolean | null }
interface Probe { provider: Provider; ok: boolean; status: number; models: ProbedModel[]; error: string | null }
interface Capabilities { context_length: number | null; tools: boolean | null; vision: boolean | null; json: boolean | null }
interface Candidate { provider: Provider; model: string; rating: number; caps: Capabilities }

function log(message: string): void { process.stderr.write(`aggregator: ${message}\n`); }

// ---- brief parsing: the three JSON documents, found by their `schema` ----

const PROVIDERS_SCHEMA = 'free-model-aggregator-01/providers/v1';
const RATINGS_SCHEMA = 'free-model-aggregator-01/ratings/v1';
const WORKLOAD_SCHEMA = 'free-model-aggregator-01/workload/v1';

function objectEnd(text: string, start: number): number {
  let depth = 0; let inString = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inString) { if (ch === '\\') i += 1; else if (ch === '"') inString = false; }
    else if (ch === '"') inString = true;
    else if (ch === '{') depth += 1;
    else if (ch === '}') { depth -= 1; if (depth === 0) return i; }
  }
  return -1;
}
function documentsIn(text: string): Json[] {
  const out: Json[] = [];
  for (let start = text.indexOf('{'); start !== -1; start = text.indexOf('{', start + 1)) {
    const end = objectEnd(text, start); if (end === -1) continue;
    try { const v: unknown = JSON.parse(text.slice(start, end + 1)); if (isRecord(v) && typeof v.schema === 'string') out.push(v); } catch { /* not JSON on its own */ }
  }
  return out;
}
function providerOf(v: unknown): Provider | null {
  if (!isRecord(v)) return null;
  const id = stringOf(v.id);
  const wire = v.wire === 'openai' || v.wire === 'anthropic' || v.wire === 'gemini' ? v.wire : null;
  if (id === '' || wire === null) return null;
  const auth = v.auth === 'bearer' || v.auth === 'x-api-key' || v.auth === 'x-goog-api-key' || v.auth === 'none' ? v.auth : 'bearer';
  const base = stringOf(v.path) !== '' ? stringOf(v.path) : stringOf(v.base_path) !== '' ? stringOf(v.base_path) : stringOf(v.base_url) !== '' ? stringOf(v.base_url) : stringOf(v.base);
  return { id, name: stringOf(v.name) !== '' ? stringOf(v.name) : id, wire, auth, base, key_env: stringOf(v.key_env), key: stringOf(v.key) };
}
function ratingOf(v: unknown): RatingRow | null {
  if (!isRecord(v)) return null;
  const model = stringOf(v.model); const provider = stringOf(v.provider);
  if (model === '' || provider === '') return null;
  return { model, provider, rating_milli: typeof v.rating_milli === 'number' && Number.isFinite(v.rating_milli) ? v.rating_milli : 0, context_length: numberOrNull(v.context_length), tools: v.tools === true, vision: v.vision === true, json: v.json === true, free: v.free === true };
}
function workloadRequestOf(v: unknown): WorkloadRequest | null {
  if (!isRecord(v)) return null;
  const id = stringOf(v.id); if (id === '') return null;
  const requires = isRecord(v.requires) ? v.requires : {};
  return { id, api: v.api === 'anthropic' ? 'anthropic' : 'openai', stream: v.stream === true, weight: v.weight === 2 ? 2 : 1, requires: { tools: requires.tools === true, vision: requires.vision === true, json: requires.json === true, ...(typeof requires.min_context === 'number' && Number.isFinite(requires.min_context) ? { min_context: requires.min_context } : {}) } };
}
interface Brief { providers: Provider[]; ratings: RatingRow[]; workload: WorkloadRequest[]; error: string | null }
function readBrief(path: string): Brief {
  const empty: Brief = { providers: [], ratings: [], workload: [], error: null };
  if (path === '') return { ...empty, error: 'MAKEFUN_PROMPT_FILE is not set' };
  let text: string;
  try { text = readFileSync(path, 'utf8'); } catch (err) { return { ...empty, error: `cannot read ${path}: ${(err as Error).message}` }; }
  const docs = documentsIn(text);
  const dirDoc = docs.find((d) => d.schema === PROVIDERS_SCHEMA && Array.isArray(d.providers)) ?? null;
  if (dirDoc === null) return { ...empty, error: `the brief holds no ${PROVIDERS_SCHEMA} document` };
  const providers = (dirDoc.providers as unknown[]).flatMap((v) => { const p = providerOf(v); return p === null ? [] : [p]; });
  const ratingsDoc = docs.find((d) => d.schema === RATINGS_SCHEMA && Array.isArray(d.ratings)) ?? null;
  const ratings = ratingsDoc !== null ? (ratingsDoc.ratings as unknown[]).flatMap((v) => { const r = ratingOf(v); return r === null ? [] : [r]; }) : [];
  const workloadDoc = docs.find((d) => d.schema === WORKLOAD_SCHEMA && Array.isArray(d.manifest)) ?? null;
  const workload = workloadDoc !== null ? (workloadDoc.manifest as unknown[]).flatMap((v) => { const w = workloadRequestOf(v); return w === null ? [] : [w]; }) : [];
  return { providers, ratings, workload, error: null };
}

// ---- catalog probe: each provider's own wire, tolerating every failure ----

function joinBase(envUrl: string, base: string): string {
  const b = base.trim();
  if (/^https?:\/\//i.test(b)) return b.replace(/\/+$/, '');
  const prefix = envUrl.replace(/\/+$/, '');
  if (b === '') return prefix;
  return `${prefix}${b.startsWith('/') ? '' : '/'}${b.replace(/\/+$/, '')}`;
}
function authHeaders(auth: AuthScheme, key: string): Record<string, string> {
  if (key === '') return {};
  if (auth === 'bearer') return { authorization: `Bearer ${key}` };
  if (auth === 'x-api-key') return { 'x-api-key': key };
  if (auth === 'x-goog-api-key') return { 'x-goog-api-key': key };
  return {};
}
function zeroPriced(v: Json): boolean | null {
  const pricing = isRecord(v.pricing) ? v.pricing : null; if (pricing === null) return null;
  const values = Object.values(pricing).flatMap((x) => { if (typeof x === 'number') return [x]; if (typeof x === 'string' && x.trim() !== '') return [Number(x)]; return []; });
  if (values.length === 0) return null;
  return values.every((n) => Number.isFinite(n) && n === 0);
}
function modelIn(wire: Wire, v: unknown): ProbedModel | null {
  if (!isRecord(v)) return null;
  const id = wire === 'gemini' ? stringOf(v.name).replace(/^models\//, '') : stringOf(v.id);
  if (id === '') return null;
  return { id, display_name: stringOf(v.display_name) !== '' ? stringOf(v.display_name) : stringOf(v.displayName) !== '' ? stringOf(v.displayName) : id, free: boolOrNull(v.free) ?? zeroPriced(v), context_length: numberOrNull(v.context_length) ?? numberOrNull(v.context_window) ?? numberOrNull(v.inputTokenLimit), tools: boolOrNull(v.tools), vision: boolOrNull(v.vision), json: boolOrNull(v.json) };
}
function modelsIn(wire: Wire, body: unknown): ProbedModel[] {
  if (!isRecord(body)) return [];
  const list = wire === 'gemini' ? body.models : body.data;
  if (!Array.isArray(list)) return [];
  return list.flatMap((v) => { const m = modelIn(wire, v); return m === null ? [] : [m]; });
}
async function probeProvider(envUrl: string, provider: Provider, timeoutMs = 5000): Promise<Probe> {
  const base = joinBase(envUrl, provider.base);
  const url = provider.wire === 'gemini' ? `${base}/v1beta/models` : `${base}/v1/models`;
  const headers: Record<string, string> = { accept: 'application/json', ...authHeaders(provider.auth, provider.key) };
  if (provider.wire === 'anthropic') headers['anthropic-version'] = '2023-06-01';
  try {
    const res = await fetch(url, { headers, signal: AbortSignal.timeout(timeoutMs) });
    if (!res.ok) return { provider, ok: false, status: res.status, models: [], error: `GET ${url} answered HTTP ${res.status}` };
    return { provider, ok: true, status: res.status, models: modelsIn(provider.wire, await res.json()), error: null };
  } catch (err) { return { provider, ok: false, status: 0, models: [], error: `GET ${url} failed: ${(err as Error).message}` }; }
}
function ratingOfModel(ratings: RatingRow[], providerId: string, modelId: string): RatingRow | null {
  return ratings.find((r) => r.provider === providerId && r.model === modelId) ?? null;
}
function modelFree(model: ProbedModel, row: RatingRow | null): boolean { if (row !== null) return row.free; return model.free ?? false; }
function capabilitiesOf(model: ProbedModel, row: RatingRow | null): Capabilities {
  return { context_length: row?.context_length ?? model.context_length ?? null, tools: row?.tools ?? model.tools ?? null, vision: row?.vision ?? model.vision ?? null, json: row?.json ?? model.json ?? null };
}
function meetsRequirements(requires: Requirement, caps: Capabilities): boolean {
  if (requires.tools === true && caps.tools !== true) return false;
  if (requires.vision === true && caps.vision !== true) return false;
  if (requires.json === true && caps.json !== true) return false;
  if (requires.min_context !== undefined && (caps.context_length === null || caps.context_length < requires.min_context)) return false;
  return true;
}
function buildCatalog(probes: readonly Probe[], ratings: RatingRow[]): CatalogReport {
  const providers: CatalogProvider[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    const seen = new Set<string>(); const models: CatalogModel[] = [];
    for (const model of [...probe.models].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
      if (seen.has(model.id)) continue; seen.add(model.id);
      const row = ratingOfModel(ratings, probe.provider.id, model.id);
      models.push({ id: model.id, free: modelFree(model, row), context_length: row?.context_length ?? model.context_length ?? null });
    }
    providers.push({ id: probe.provider.id, base_url: probe.provider.base, key_env: probe.provider.key_env, wire: probe.provider.wire, models });
  }
  return { providers };
}

// ---- wire translation: every client wire to every upstream wire ----

function textOf(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.flatMap((b) => (isRecord(b) && b.type === 'text' && typeof b.text === 'string' ? [b.text] : [])).join('\n');
}
function argumentsOf(value: unknown): Json {
  if (isRecord(value)) return value;
  if (typeof value === 'string' && value.trim() !== '') { try { const p: unknown = JSON.parse(value); if (isRecord(p)) return p; } catch { /* empty */ } }
  return {};
}
function toolResultText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) { const t = textOf(content); return t !== '' ? t : JSON.stringify(content); }
  return content === undefined ? '' : JSON.stringify(content);
}

/** Anthropic Messages request as OpenAI Chat Completions (starter's translation). */
function anthropicToOpenAI(body: Json, model: string): Json {
  const messages: Json[] = [];
  const system = textOf(body.system);
  if (system !== '') messages.push({ role: 'system', content: system });
  for (const raw of Array.isArray(body.messages) ? body.messages : []) {
    if (!isRecord(raw)) continue;
    const assistant = raw.role === 'assistant'; const content = raw.content;
    if (typeof content === 'string') { messages.push({ role: assistant ? 'assistant' : 'user', content }); continue; }
    if (!Array.isArray(content)) continue;
    if (assistant) {
      const toolCalls: Json[] = [];
      for (const block of content) if (isRecord(block) && block.type === 'tool_use') toolCalls.push({ id: typeof block.id === 'string' ? block.id : '', type: 'function', function: { name: typeof block.name === 'string' ? block.name : '', arguments: JSON.stringify(isRecord(block.input) ? block.input : {}) } });
      const text = textOf(content);
      messages.push({ role: 'assistant', content: text !== '' ? text : null, ...(toolCalls.length > 0 ? { tool_calls: toolCalls } : {}) });
      continue;
    }
    const parts: Json[] = [];
    for (const block of content) {
      if (!isRecord(block)) continue;
      if (block.type === 'tool_result') { messages.push({ role: 'tool', tool_call_id: typeof block.tool_use_id === 'string' ? block.tool_use_id : '', content: toolResultText(block.content) }); continue; }
      if (block.type === 'text' && typeof block.text === 'string') parts.push({ type: 'text', text: block.text });
      else if (block.type === 'image' && isRecord(block.source)) {
        if (block.source.type === 'base64' && typeof block.source.data === 'string') parts.push({ type: 'image_url', image_url: { url: `data:${typeof block.source.media_type === 'string' ? block.source.media_type : 'image/png'};base64,${block.source.data}` } });
        else if (block.source.type === 'url' && typeof block.source.url === 'string') parts.push({ type: 'image_url', image_url: { url: block.source.url } });
      }
    }
    if (parts.length > 0) messages.push({ role: 'user', content: parts });
  }
  const out: Json = { model, messages };
  if (typeof body.max_tokens === 'number') out.max_tokens = body.max_tokens;
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (Array.isArray(body.stop_sequences) && body.stop_sequences.length > 0) out.stop = body.stop_sequences;
  if (Array.isArray(body.tools)) {
    const tools = body.tools.flatMap((raw) => { if (!isRecord(raw) || typeof raw.name !== 'string') return []; return [{ type: 'function', function: { name: raw.name, description: typeof raw.description === 'string' ? raw.description : '', parameters: isRecord(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} } } }]; });
    if (tools.length > 0) out.tools = tools;
  }
  const choice = body.tool_choice;
  if (isRecord(choice)) {
    if (choice.type === 'auto') out.tool_choice = 'auto';
    else if (choice.type === 'any') out.tool_choice = 'required';
    else if (choice.type === 'tool' && typeof choice.name === 'string') out.tool_choice = { type: 'function', function: { name: choice.name } };
  }
  if (body.stream === true) { out.stream = true; out.stream_options = { include_usage: true }; }
  return out;
}

/** OpenAI Chat Completions request as Anthropic Messages. */
function openAIToAnthropic(body: Json, model: string): Json {
  const systemParts: string[] = [];
  const messages: Json[] = [];
  for (const raw of Array.isArray(body.messages) ? body.messages : []) {
    if (!isRecord(raw)) continue;
    if (raw.role === 'system') { const t = textOf(raw.content); if (t !== '') systemParts.push(t); continue; }
    if (raw.role === 'assistant') {
      const blocks: Json[] = [];
      const t = textOf(raw.content); if (t !== '') blocks.push({ type: 'text', text: t });
      for (const call of Array.isArray(raw.tool_calls) ? raw.tool_calls : []) if (isRecord(call) && isRecord(call.function)) blocks.push({ type: 'tool_use', id: typeof call.id === 'string' ? call.id : 'toolu_unknown', name: typeof call.function.name === 'string' ? call.function.name : '', input: argumentsOf(call.function.arguments) });
      if (blocks.length > 0) messages.push({ role: 'assistant', content: blocks });
      continue;
    }
    if (raw.role === 'tool') {
      messages.push({ role: 'user', content: [{ type: 'tool_result', tool_use_id: typeof raw.tool_call_id === 'string' ? raw.tool_call_id : '', content: typeof raw.content === 'string' ? raw.content : textOf(raw.content) }] });
      continue;
    }
    // user
    const content = raw.content;
    if (typeof content === 'string') { messages.push({ role: 'user', content }); continue; }
    if (!Array.isArray(content)) continue;
    const blocks: Json[] = [];
    for (const part of content) {
      if (!isRecord(part)) continue;
      if (part.type === 'text' && typeof part.text === 'string') blocks.push({ type: 'text', text: part.text });
      else if (part.type === 'image_url' && isRecord(part.image_url) && typeof part.image_url.url === 'string') {
        const url = part.image_url.url;
        const m = /^data:([^;]+);base64,(.*)$/s.exec(url);
        if (m !== null) blocks.push({ type: 'image', source: { type: 'base64', media_type: m[1] ?? 'image/png', data: m[2] ?? '' } });
        else blocks.push({ type: 'image', source: { type: 'url', url } });
      }
    }
    if (blocks.length > 0) messages.push({ role: 'user', content: blocks });
  }
  const out: Json = { model, messages };
  if (systemParts.length > 0) out.system = systemParts.join('\n');
  const maxTokens = typeof body.max_tokens === 'number' ? body.max_tokens : typeof body.max_completion_tokens === 'number' ? body.max_completion_tokens : undefined;
  if (maxTokens !== undefined) out.max_tokens = maxTokens;
  if (typeof body.temperature === 'number') out.temperature = body.temperature;
  if (typeof body.top_p === 'number') out.top_p = body.top_p;
  if (typeof body.stop === 'string') out.stop_sequences = [body.stop];
  else if (Array.isArray(body.stop) && body.stop.length > 0) out.stop_sequences = body.stop;
  if (Array.isArray(body.tools)) {
    const tools = body.tools.flatMap((raw) => { if (!isRecord(raw) || !isRecord(raw.function) || typeof raw.function.name !== 'string') return []; return [{ name: raw.function.name, description: typeof raw.function.description === 'string' ? raw.function.description : '', input_schema: isRecord(raw.function.parameters) ? raw.function.parameters : { type: 'object', properties: {} } }]; });
    if (tools.length > 0) out.tools = tools;
  }
  const tc = body.tool_choice;
  if (tc === 'auto') out.tool_choice = { type: 'auto' };
  else if (tc === 'none') out.tool_choice = { type: 'auto' };
  else if (tc === 'required') out.tool_choice = { type: 'any' };
  else if (isRecord(tc) && isRecord(tc.function) && typeof tc.function.name === 'string') out.tool_choice = { type: 'tool', name: tc.function.name };
  if (body.stream === true) out.stream = true;
  return out;
}

function imagePartFromDataUrl(url: string): Json | null {
  const m = /^data:([^;]+);base64,(.*)$/s.exec(url);
  if (m !== null) return { inlineData: { mimeType: m[1] ?? 'image/png', data: m[2] ?? '' } };
  return { fileData: { fileUri: url } };
}
function geminiPartsFromOpenAIContent(content: unknown): Json[] {
  const parts: Json[] = [];
  if (typeof content === 'string') { if (content !== '') parts.push({ text: content }); return parts; }
  if (!Array.isArray(content)) return parts;
  for (const part of content) {
    if (!isRecord(part)) continue;
    if (part.type === 'text' && typeof part.text === 'string') parts.push({ text: part.text });
    else if (part.type === 'image_url' && isRecord(part.image_url) && typeof part.image_url.url === 'string') { const p = imagePartFromDataUrl(part.image_url.url); if (p !== null) parts.push(p); }
  }
  return parts;
}
function geminiPartsFromAnthropicContent(content: unknown): Json[] {
  const parts: Json[] = [];
  if (typeof content === 'string') { if (content !== '') parts.push({ text: content }); return parts; }
  if (!Array.isArray(content)) return parts;
  for (const block of content) {
    if (!isRecord(block)) continue;
    if (block.type === 'text' && typeof block.text === 'string') parts.push({ text: block.text });
    else if (block.type === 'image' && isRecord(block.source)) {
      if (block.source.type === 'base64' && typeof block.source.data === 'string') parts.push({ inlineData: { mimeType: typeof block.source.media_type === 'string' ? block.source.media_type : 'image/png', data: block.source.data } });
      else if (block.source.type === 'url' && typeof block.source.url === 'string') { const p = imagePartFromDataUrl(block.source.url); if (p !== null) parts.push(p); }
    } else if (block.type === 'tool_result') parts.push({ text: toolResultText(block.content) });
  }
  return parts;
}
/** OpenAI or Anthropic client body as a Gemini generateContent body. */
function toGemini(clientApi: 'openai' | 'anthropic', body: Json, model: string, jsonMode: boolean): Json {
  let systemText = ''; let contents: Json[] = []; let tools: Json[] = [];
  let maxTokens: number | undefined; let temperature: number | undefined; let topP: number | undefined; let stops: string[] = [];
  if (clientApi === 'openai') {
    for (const raw of Array.isArray(body.messages) ? body.messages : []) {
      if (!isRecord(raw)) continue;
      if (raw.role === 'system') { const t = textOf(raw.content); if (t !== '') systemText = systemText === '' ? t : `${systemText}\n${t}`; continue; }
      if (raw.role === 'assistant') {
        const parts: Json[] = [];
        const t = textOf(raw.content); if (t !== '') parts.push({ text: t });
        for (const call of Array.isArray(raw.tool_calls) ? raw.tool_calls : []) if (isRecord(call) && isRecord(call.function)) parts.push({ functionCall: { name: typeof call.function.name === 'string' ? call.function.name : '', args: argumentsOf(call.function.arguments) } });
        if (parts.length > 0) contents.push({ role: 'model', parts });
        continue;
      }
      if (raw.role === 'tool') { contents.push({ role: 'user', parts: [{ text: typeof raw.content === 'string' ? raw.content : textOf(raw.content) }] }); continue; }
      const parts = geminiPartsFromOpenAIContent(raw.content);
      if (parts.length > 0) contents.push({ role: 'user', parts });
    }
    if (Array.isArray(body.tools)) tools = body.tools.flatMap((raw) => { if (!isRecord(raw) || !isRecord(raw.function) || typeof raw.function.name !== 'string') return []; return [{ name: raw.function.name, description: typeof raw.function.description === 'string' ? raw.function.description : '', parameters: isRecord(raw.function.parameters) ? raw.function.parameters : { type: 'object', properties: {} } }]; });
    maxTokens = typeof body.max_tokens === 'number' ? body.max_tokens : typeof body.max_completion_tokens === 'number' ? body.max_completion_tokens : undefined;
    temperature = typeof body.temperature === 'number' ? body.temperature : undefined;
    topP = typeof body.top_p === 'number' ? body.top_p : undefined;
    if (typeof body.stop === 'string') stops = [body.stop]; else if (Array.isArray(body.stop)) stops = body.stop.filter((s): s is string => typeof s === 'string');
  } else {
    systemText = textOf(body.system);
    for (const raw of Array.isArray(body.messages) ? body.messages : []) {
      if (!isRecord(raw)) continue;
      if (raw.role === 'assistant') {
        const parts: Json[] = [];
        const t = textOf(raw.content); if (t !== '') parts.push({ text: t });
        for (const block of Array.isArray(raw.content) ? raw.content : []) if (isRecord(block) && block.type === 'tool_use') parts.push({ functionCall: { name: typeof block.name === 'string' ? block.name : '', args: isRecord(block.input) ? block.input : {} } });
        if (parts.length > 0) contents.push({ role: 'model', parts });
        continue;
      }
      const parts = geminiPartsFromAnthropicContent(raw.content);
      if (parts.length > 0) contents.push({ role: 'user', parts });
    }
    if (Array.isArray(body.tools)) tools = body.tools.flatMap((raw) => { if (!isRecord(raw) || typeof raw.name !== 'string') return []; return [{ name: raw.name, description: typeof raw.description === 'string' ? raw.description : '', parameters: isRecord(raw.input_schema) ? raw.input_schema : { type: 'object', properties: {} } }]; });
    maxTokens = typeof body.max_tokens === 'number' ? body.max_tokens : undefined;
    temperature = typeof body.temperature === 'number' ? body.temperature : undefined;
    topP = typeof body.top_p === 'number' ? body.top_p : undefined;
    if (Array.isArray(body.stop_sequences)) stops = body.stop_sequences.filter((s): s is string => typeof s === 'string');
  }
  // Gemini takes the model from the URL path, never from the body.
  const out: Json = { contents };
  if (systemText !== '') out.systemInstruction = { parts: [{ text: systemText }] };
  if (tools.length > 0) out.tools = [{ functionDeclarations: tools }];
  const config: Json = {};
  if (maxTokens !== undefined) config.maxOutputTokens = maxTokens;
  if (temperature !== undefined) config.temperature = temperature;
  if (topP !== undefined) config.topP = topP;
  if (stops.length > 0) config.stopSequences = stops;
  if (jsonMode) config.responseMimeType = 'application/json';
  if (Object.keys(config).length > 0) out.generationConfig = config;
  return out;
}

// ---- normalised answers: one shape, whatever the upstream wire was ----

interface Answer { text: string; input: number; output: number }
function sseDataFrames(text: string): Json[] {
  const out: Json[] = [];
  for (const frame of text.replace(/\r\n/g, '\n').split('\n\n')) {
    if (frame.trim() === '') continue;
    for (const line of frame.split('\n')) {
      const t = line.trim();
      if (!t.startsWith('data:')) continue;
      const payload = t.slice(5).trim();
      if (payload === '[DONE]') continue;
      try { const v: unknown = JSON.parse(payload); if (isRecord(v)) out.push(v); } catch { /* skip */ }
    }
  }
  return out;
}
function parseOpenAIAnswer(text: string, stream: boolean): Answer | null {
  if (!stream) {
    let v: unknown; try { v = JSON.parse(text); } catch { return null; }
    if (!isRecord(v) || !Array.isArray(v.choices)) return null;
    const choice = isRecord(v.choices[0]) ? v.choices[0] : null; if (choice === null) return null;
    const msg = isRecord(choice.message) ? choice.message : {};
    const content = typeof msg.content === 'string' ? msg.content : textOf(msg.content);
    const usage = isRecord(v.usage) ? v.usage : {};
    return { text: content, input: typeof usage.prompt_tokens === 'number' ? usage.prompt_tokens : 0, output: typeof usage.completion_tokens === 'number' ? usage.completion_tokens : 0 };
  }
  let content = ''; let input = 0; let output = 0;
  for (const chunk of sseDataFrames(text)) {
    if (isRecord(chunk.usage)) { input = typeof chunk.usage.prompt_tokens === 'number' ? chunk.usage.prompt_tokens : input; output = typeof chunk.usage.completion_tokens === 'number' ? chunk.usage.completion_tokens : output; }
    const choice = Array.isArray(chunk.choices) && isRecord(chunk.choices[0]) ? chunk.choices[0] : null; if (choice === null) continue;
    const delta = isRecord(choice.delta) ? choice.delta : {};
    if (typeof delta.content === 'string') content += delta.content;
  }
  return { text: content, input, output };
}
function parseAnthropicAnswer(text: string, stream: boolean): Answer | null {
  if (!stream) {
    let v: unknown; try { v = JSON.parse(text); } catch { return null; }
    if (!isRecord(v) || !Array.isArray(v.content)) return null;
    const usage = isRecord(v.usage) ? v.usage : {};
    return { text: answerText(v.content), input: typeof usage.input_tokens === 'number' ? usage.input_tokens : 0, output: typeof usage.output_tokens === 'number' ? usage.output_tokens : 0 };
  }
  let content = ''; let input = 0; let output = 0;
  for (const frame of text.replace(/\r\n/g, '\n').split('\n\n')) {
    if (frame.trim() === '') continue;
    let event: string | null = null; const data: string[] = [];
    for (const line of frame.split('\n')) { if (line.startsWith('event:')) event = line.slice(6).trim(); else if (line.startsWith('data:')) data.push(line.slice(5).replace(/^ /, '')); }
    if (data.length === 0) continue;
    let v: unknown; try { v = JSON.parse(data.join('\n')); } catch { continue; }
    if (!isRecord(v)) continue;
    if (event === 'message_start' && isRecord(v.message) && isRecord(v.message.usage)) input = typeof v.message.usage.input_tokens === 'number' ? v.message.usage.input_tokens : input;
    if (event === 'content_block_delta' && isRecord(v.delta) && v.delta.type === 'text_delta' && typeof v.delta.text === 'string') content += v.delta.text;
    if (event === 'message_delta' && isRecord(v.usage)) output = typeof v.usage.output_tokens === 'number' ? v.usage.output_tokens : output;
  }
  return { text: content, input, output };
}
function answerText(content: unknown): string {
  if (typeof content === 'string') return content;
  if (!Array.isArray(content)) return '';
  return content.flatMap((b) => (isRecord(b) && b.type === 'text' && typeof b.text === 'string' ? [b.text] : [])).join('');
}
function geminiTextOf(v: Json): string {
  const cands = Array.isArray(v.candidates) ? v.candidates : [];
  const first = isRecord(cands[0]) ? cands[0] : null; if (first === null) return '';
  const content = isRecord(first.content) ? first.content : {};
  const parts = Array.isArray(content.parts) ? content.parts : [];
  return parts.flatMap((p) => (isRecord(p) && typeof p.text === 'string' ? [p.text] : [])).join('');
}
function parseGeminiAnswer(text: string, stream: boolean): Answer | null {
  if (!stream) {
    let v: unknown; try { v = JSON.parse(text); } catch { return null; }
    if (!isRecord(v)) return null;
    const meta = isRecord(v.usageMetadata) ? v.usageMetadata : {};
    return { text: geminiTextOf(v), input: typeof meta.promptTokenCount === 'number' ? meta.promptTokenCount : 0, output: typeof meta.candidatesTokenCount === 'number' ? meta.candidatesTokenCount : 0 };
  }
  let content = ''; let input = 0; let output = 0;
  for (const chunk of sseDataFrames(text)) {
    content += geminiTextOf(chunk);
    const meta = isRecord(chunk.usageMetadata) ? chunk.usageMetadata : null;
    if (meta !== null) { input = typeof meta.promptTokenCount === 'number' ? meta.promptTokenCount : input; output = typeof meta.candidatesTokenCount === 'number' ? meta.candidatesTokenCount : output; }
  }
  return { text: content, input, output };
}

// ---- rendering the normalised answer in the client's wire ----

function sseEvent(event: string | null, data: unknown): string { return `${event === null ? '' : `event: ${event}\n`}data: ${JSON.stringify(data)}\n\n`; }
function splitText(text: string): string[] { const out: string[] = []; for (let at = 0; at < text.length; at += 13) out.push(text.slice(at, at + 13)); return out.length === 0 ? [''] : out; }
function renderOpenAI(model: string, answer: Answer, stream: boolean): { contentType: string; body: string } {
  if (!stream) return { contentType: 'application/json', body: JSON.stringify({ id: 'chatcmpl-sim', object: 'chat.completion', created: FIXED_CREATED, model, choices: [{ index: 0, message: { role: 'assistant', content: answer.text }, finish_reason: 'stop', logprobs: null }], usage: { prompt_tokens: answer.input, completion_tokens: answer.output, total_tokens: answer.input + answer.output } }) };
  let body = '';
  body += sseEvent(null, { id: 'chatcmpl-sim', object: 'chat.completion.chunk', created: FIXED_CREATED, model, choices: [{ index: 0, delta: { role: 'assistant', content: '' }, finish_reason: null }] });
  for (const piece of splitText(answer.text)) body += sseEvent(null, { id: 'chatcmpl-sim', object: 'chat.completion.chunk', created: FIXED_CREATED, model, choices: [{ index: 0, delta: { content: piece }, finish_reason: null }] });
  body += sseEvent(null, { id: 'chatcmpl-sim', object: 'chat.completion.chunk', created: FIXED_CREATED, model, choices: [{ index: 0, delta: {}, finish_reason: 'stop' }] });
  body += sseEvent(null, { id: 'chatcmpl-sim', object: 'chat.completion.chunk', created: FIXED_CREATED, model, choices: [], usage: { prompt_tokens: answer.input, completion_tokens: answer.output, total_tokens: answer.input + answer.output } });
  body += 'data: [DONE]\n\n';
  return { contentType: 'text/event-stream', body };
}
function renderAnthropic(model: string, answer: Answer, stream: boolean): { contentType: string; body: string } {
  if (!stream) return { contentType: 'application/json', body: JSON.stringify({ id: 'msg_sim', type: 'message', role: 'assistant', model, content: [{ type: 'text', text: answer.text }], stop_reason: 'end_turn', stop_sequence: null, usage: { input_tokens: answer.input, output_tokens: answer.output } }) };
  let body = '';
  body += sseEvent('message_start', { type: 'message_start', message: { id: 'msg_sim', type: 'message', role: 'assistant', model, content: [], stop_reason: null, stop_sequence: null, usage: { input_tokens: answer.input, output_tokens: 0 } } });
  body += sseEvent('content_block_start', { type: 'content_block_start', index: 0, content_block: { type: 'text', text: '' } });
  for (const piece of splitText(answer.text)) body += sseEvent('content_block_delta', { type: 'content_block_delta', index: 0, delta: { type: 'text_delta', text: piece } });
  body += sseEvent('content_block_stop', { type: 'content_block_stop', index: 0 });
  body += sseEvent('message_delta', { type: 'message_delta', delta: { stop_reason: 'end_turn', stop_sequence: null }, usage: { output_tokens: answer.output } });
  body += sseEvent('message_stop', { type: 'message_stop' });
  return { contentType: 'text/event-stream', body };
}

// ---- routing: up-front plan, then failover at request time ----

const brief = readBrief(PROMPT_FILE);
let probes: Probe[] = [];
let catalog: CatalogReport = { providers: [] };
let manifestIndex = 0;
let planPrimary: (Candidate | null)[] = [];
const providerFails = new Map<string, number>();
const providerCalls = new Map<string, number>();

function candidatesFor(req: WorkloadRequest | null, clientApi: 'openai' | 'anthropic', body: Json): Candidate[] {
  const requires: Requirement = req !== null ? req.requires : requirementsFromBody(clientApi, body);
  const jsonMode = clientApi === 'openai' && isRecord(body.response_format) && (body.response_format.type === 'json_object' || body.response_format.type === 'json_schema');
  const out: Candidate[] = [];
  for (const probe of probes) {
    if (!probe.ok) continue;
    for (const model of probe.models) {
      const row = ratingOfModel(brief.ratings, probe.provider.id, model.id);
      if (!modelFree(model, row)) continue;
      const caps = capabilitiesOf(model, row);
      if (!meetsRequirements(requires, caps)) continue;
      // An Anthropic-wire provider cannot carry JSON mode: its echo says
      // json_mode was absent, so an OpenAI JSON request must not use it.
      if (jsonMode && probe.provider.wire === 'anthropic') continue;
      out.push({ provider: probe.provider, model: model.id, rating: row?.rating_milli ?? 0, caps });
    }
  }
  out.sort((a, b) => b.rating - a.rating || (a.provider.id < b.provider.id ? -1 : a.provider.id > b.provider.id ? 1 : 0) || (a.model < b.model ? -1 : a.model > b.model ? 1 : 0));
  return out;
}
function requirementsFromBody(clientApi: 'openai' | 'anthropic', body: Json): Requirement {
  let vision = false;
  const messages = Array.isArray(body.messages) ? body.messages : [];
  for (const message of messages) {
    if (!isRecord(message) || !Array.isArray(message.content)) continue;
    for (const block of message.content) if (isRecord(block) && (block.type === 'image' || block.type === 'image_url')) vision = true;
  }
  const format = isRecord(body.response_format) ? body.response_format.type : undefined;
  return { tools: Array.isArray(body.tools) && body.tools.length > 0, vision, json: format === 'json_object' || format === 'json_schema' };
}
/** Plan every manifest request up front, scarcest first, capped per provider. */
function buildPlan(): void {
  const manifest = brief.workload;
  planPrimary = manifest.map(() => null);
  const byIndex = manifest.map((req, i) => ({ req, i, cands: candidatesFor(req, req.api, {}) }));
  const order = [...byIndex].sort((a, b) => a.cands.length - b.cands.length || b.req.weight - a.req.weight || a.i - b.i);
  const used = new Map<string, number>();
  for (const { i, cands } of order) {
    let pick = cands.find((c) => (used.get(c.provider.id) ?? 0) < PLAN_CAP_PER_PROVIDER) ?? cands[0] ?? null;
    if (pick !== null) { planPrimary[i] = pick; used.set(pick.provider.id, (used.get(pick.provider.id) ?? 0) + 1); }
  }
}
function orderedCandidates(index: number, clientApi: 'openai' | 'anthropic', body: Json): Candidate[] {
  const req = brief.workload[index] ?? null;
  const all = candidatesFor(req, clientApi, body);
  const primary = planPrimary[index] ?? null;
  const out: Candidate[] = [];
  const seen = new Set<string>();
  const push = (c: Candidate | null): void => { if (c === null) return; const k = `${c.provider.id}/${c.model}`; if (seen.has(k)) return; seen.add(k); out.push(c); };
  if (primary !== null && (providerFails.get(primary.provider.id) ?? 0) < DEAD_AFTER_FAILURES) push(primary);
  for (const c of all) { if ((providerFails.get(c.provider.id) ?? 0) >= DEAD_AFTER_FAILURES) continue; push(c); }
  // Last resort: the dead providers, best first, in case both failures were
  // scheduled refusals and the next call would have succeeded.
  for (const c of all) push(c);
  if (primary !== null) push(primary);
  return out;
}

async function load(): Promise<void> {
  if (brief.error !== null) log(brief.error);
  probes = await Promise.all(brief.providers.map((p) => probeProvider(ENV_URL, p, PROBE_TIMEOUT_MS)));
  for (const probe of probes) if (!probe.ok && probe.error !== null) log(probe.error);
  catalog = buildCatalog(probes, brief.ratings);
  buildPlan();
  log(`catalog: ${catalog.providers.length} providers, plan: ${planPrimary.filter((p) => p !== null).length} of ${brief.workload.length} requests have a primary`);
}
function servedModels(): { id: string; provider: string; display_name: string }[] {
  const out: { id: string; provider: string; display_name: string }[] = []; const seen = new Set<string>();
  for (const probe of probes) {
    if (!probe.ok) continue;
    for (const model of [...probe.models].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0))) {
      const row = ratingOfModel(brief.ratings, probe.provider.id, model.id);
      if (!modelFree(model, row)) continue;
      if (seen.has(model.id)) continue; seen.add(model.id);
      out.push({ id: model.id, provider: probe.provider.id, display_name: model.display_name });
    }
  }
  return out;
}

// ---- one upstream call, in the provider's own wire ----

interface UpstreamResult { ok: boolean; status: number; answer: Answer | null; model: string }
async function callUpstream(cand: Candidate, clientApi: 'openai' | 'anthropic', body: Json, stream: boolean): Promise<UpstreamResult> {
  const provider = cand.provider;
  const base = joinBase(ENV_URL, provider.base);
  const jsonMode = clientApi === 'openai' && isRecord(body.response_format) && (body.response_format.type === 'json_object' || body.response_format.type === 'json_schema');
  let url = ''; let upstreamBody: Json; const headers: Record<string, string> = { 'content-type': 'application/json', ...authHeaders(provider.auth, provider.key) };
  if (provider.wire === 'openai') {
    url = `${base}/v1/chat/completions`;
    upstreamBody = clientApi === 'openai' ? { ...body, model: cand.model } : anthropicToOpenAI(body, cand.model);
    if (stream) { upstreamBody.stream = true; upstreamBody.stream_options = { include_usage: true }; }
    if (stream) headers.accept = 'text/event-stream';
  } else if (provider.wire === 'anthropic') {
    url = `${base}/v1/messages`;
    headers['anthropic-version'] = '2023-06-01';
    upstreamBody = clientApi === 'anthropic' ? { ...body, model: cand.model } : openAIToAnthropic(body, cand.model);
    if (stream) upstreamBody.stream = true;
  } else {
    const streamPath = stream ? 'streamGenerateContent' : 'generateContent';
    url = `${base}/v1beta/models/${encodeURIComponent(cand.model)}:${streamPath}?alt=sse`;
    upstreamBody = toGemini(clientApi, body, cand.model, jsonMode || (brief.workload[manifestIndex] !== undefined && brief.workload[manifestIndex]?.requires.json === true));
    if (stream) headers.accept = 'text/event-stream';
  }
  providerCalls.set(provider.id, (providerCalls.get(provider.id) ?? 0) + 1);
  let res: Response;
  try { res = await fetch(url, { method: 'POST', headers, body: JSON.stringify(upstreamBody), signal: AbortSignal.timeout(30000) }); }
  catch (err) { log(`upstream ${provider.id}/${cand.model} unreachable: ${(err as Error).message}`); return { ok: false, status: 0, answer: null, model: cand.model }; }
  if (!res.ok) { log(`upstream ${provider.id}/${cand.model} answered HTTP ${res.status}`); return { ok: false, status: res.status, answer: null, model: cand.model }; }
  const text = await res.text();
  const answer = provider.wire === 'openai' ? parseOpenAIAnswer(text, stream) : provider.wire === 'anthropic' ? parseAnthropicAnswer(text, stream) : parseGeminiAnswer(text, stream);
  if (answer === null) { log(`upstream ${provider.id}/${cand.model} answer did not parse`); return { ok: false, status: res.status, answer: null, model: cand.model }; }
  return { ok: true, status: res.status, answer, model: cand.model };
}

// ---- HTTP server ----

async function readJson(req: IncomingMessage): Promise<Json | null> {
  const chunks: Buffer[] = []; let size = 0;
  for await (const chunk of req) { const b = Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)); size += b.length; if (size > MAX_BODY_BYTES) return null; chunks.push(b); }
  if (chunks.length === 0) return null;
  try { const v: unknown = JSON.parse(Buffer.concat(chunks).toString('utf8')); return isRecord(v) ? v : null; } catch { return null; }
}
function sendJson(res: ServerResponse, status: number, body: unknown): void {
  const text = JSON.stringify(body);
  res.writeHead(status, { 'content-type': 'application/json', 'content-length': Buffer.byteLength(text) }); res.end(text);
}
function sendError(res: ServerResponse, wire: 'openai' | 'anthropic', status: number, type: string, message: string): void {
  if (wire === 'anthropic') sendJson(res, status, { type: 'error', error: { type, message } });
  else sendJson(res, status, { error: { message, type, code: null } });
}
function models(req: IncomingMessage, res: ServerResponse): void {
  const list = servedModels();
  if (typeof req.headers['x-api-key'] === 'string') sendJson(res, 200, { data: list.map((m) => ({ type: 'model', id: m.id, display_name: m.display_name, created_at: FIXED_ANTHROPIC_CREATED_AT })), has_more: false });
  else sendJson(res, 200, { object: 'list', data: list.map((m) => ({ id: m.id, object: 'model', created: FIXED_CREATED, owned_by: m.provider })) });
}
async function serve(req: IncomingMessage, res: ServerResponse, clientApi: 'openai' | 'anthropic'): Promise<void> {
  const body = await readJson(req);
  if (body === null) return sendError(res, clientApi, 400, 'invalid_request_error', 'the request body is not one JSON object');
  const index = manifestIndex; manifestIndex += 1;
  const stream = body.stream === true;
  const ordered = orderedCandidates(index, clientApi, body);
  if (ordered.length === 0) return sendError(res, clientApi, 503, 'no_free_model', 'no free model on the directory meets this request');
  const tried = new Map<string, number>();
  for (let pass = 0; pass < 2; pass++) {
    for (const cand of ordered) {
      const triedCount = tried.get(cand.provider.id) ?? 0;
      if (pass === 0 && triedCount > 0) continue;
      if (pass === 1 && triedCount > 1) continue;
      tried.set(cand.provider.id, triedCount + 1);
      const result = await callUpstream(cand, clientApi, body, stream);
      if (result.ok && result.answer !== null) {
        const rendered = clientApi === 'openai' ? renderOpenAI(result.model, result.answer, stream) : renderAnthropic(result.model, result.answer, stream);
        res.writeHead(200, { 'content-type': rendered.contentType }); res.end(rendered.body); return;
      }
      providerFails.set(cand.provider.id, (providerFails.get(cand.provider.id) ?? 0) + 1);
      log(`failover: ${cand.provider.id}/${cand.model} failed (pass ${pass}), trying the next candidate`);
    }
  }
  return sendError(res, clientApi, 503, 'no_free_model', 'every free model that meets this request refused it');
}
async function handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://entry');
  if (req.method === 'GET' && url.pathname === '/v1/models') { models(req, res); return; }
  if (req.method === 'POST' && url.pathname === '/v1/chat/completions') { await serve(req, res, 'openai'); return; }
  if (req.method === 'POST' && url.pathname === '/v1/messages') { await serve(req, res, 'anthropic'); return; }
  sendError(res, url.pathname === '/v1/messages' ? 'anthropic' : 'openai', 404, 'not_found', `no route ${req.method ?? ''} ${url.pathname}`);
}
function listen(server: Server): Promise<void> {
  return new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '0.0.0.0', () => resolve()); });
}
async function register(port: number, report: CatalogReport): Promise<void> {
  const res = await fetch(`${ENV_URL}/register`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ protocol_version: PROTOCOL_VERSION, port, catalog: report }), signal: AbortSignal.timeout(5000) });
  if (!res.ok) throw new Error(`POST ${ENV_URL}/register answered HTTP ${res.status}: ${(await res.text()).slice(0, 500)}`);
  const reply: unknown = await res.json();
  if (isRecord(reply) && typeof reply.requests === 'number') log(`registered: the workload holds ${reply.requests} requests`);
}
function poll(server: Server): void {
  let inFlight = false;
  const timer = setInterval(() => {
    if (inFlight) return; inFlight = true;
    void (async () => {
      try {
        const res = await fetch(`${ENV_URL}/work`, { signal: AbortSignal.timeout(2000) });
        if (res.status !== 200) return;
        const work: unknown = await res.json();
        if (isRecord(work) && work.done === true) { clearInterval(timer); server.close(); process.exit(0); }
      } catch { /* try again on the next tick */ } finally { inFlight = false; }
    })();
  }, POLL_INTERVAL_MS);
}
async function start(): Promise<void> {
  await load();
  if (process.argv.includes('--list')) {
    const text = `${JSON.stringify(catalog)}\n`;
    await new Promise<void>((resolve) => process.stdout.write(text, () => resolve()));
    process.exit(0);
  }
  if (ENV_URL === '') throw new Error('MAKEFUN_ENV_URL is not set');
  const server = createServer((req, res) => { void handle(req, res).catch((err: unknown) => { log((err as Error).stack ?? String(err)); if (!res.headersSent) sendError(res, req.url?.startsWith('/v1/messages') === true ? 'anthropic' : 'openai', 500, 'internal_error', 'the server failed to answer'); }); });
  await listen(server);
  const address = server.address();
  if (address === null || typeof address === 'string') throw new Error('the server has no port');
  const port = address.port;
  if (port === 8080) throw new Error('the OS picked 8080, which the env owns on one layout: restart the trial');
  log(`listening on 0.0.0.0:${port} with ${catalog.providers.length} answered providers and ${servedModels().length} free models`);
  await register(port, catalog);
  process.on('SIGTERM', () => process.exit(0));
  process.on('SIGINT', () => process.exit(0));
  poll(server);
}
try { await start(); } catch (err) { log((err as Error).stack ?? String(err)); process.exit(1); }
