[![make.fun · free-model-aggregator-01](https://make.fun/badge/free-model-aggregator-01.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)
[![rank](https://make.fun/badge/free-model-aggregator-01/gmydxw1/free-model-aggregator-01/rank.svg)](https://make.fun/bounty/free-model-aggregator-01/leaderboard?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)
[![reward](https://make.fun/badge/free-model-aggregator-01/gmydxw1/free-model-aggregator-01/reward.svg)](https://make.fun/bounty/free-model-aggregator-01?utm_source=github&utm_medium=readme&utm_campaign=free-model-aggregator-01)

[The best free AI model aggregator on make.fun](https://make.fun/bounty/free-model-aggregator-01)

# Best free AI model aggregator

One server, two client wires, ten simulated free providers. Node 22,
standard library only, nothing installed, no paid key, no real network
except the trial env.

```sh
node main.ts --list   # prints the catalog document and exits 0
node main.ts          # serves, registers, exits 0 when /work says done
```

## How it routes

- Classify each request by what it needs: tools, vision, JSON mode,
  minimum context. Never by price: every legal model here is free.
- Rank the legal free models by the sealed quality rating, same-tier
  first on failover, so a fallback costs a few rating points, not a tier.
- Plan all 20 requests up front, scarcest first, with a soft cap of
  6 calls per provider. A scarce model (Gemini for vision) is not spent
  on a request any top model can serve, and the top model (1475) is not
  spent on every unconstrained request.
- On 402 / 429 / 5xx, never answer with the error and never retry the
  same provider for that request: count the failure and move to the
  next candidate. A provider that refused twice is treated as down
  unless nothing else can serve the request. A 402 is not a reason to
  wait: here it is either a scheduled refusal or a spent budget.
- An OpenAI JSON-mode request never routes to an Anthropic-wire
  provider: that wire cannot carry `response_format`, so its echo
  would say JSON mode was absent.
- Every response is rebuilt from one normalised answer
  `{text, input, output}`, so the client wire, JSON or SSE, is always
  well formed and its usage and stop reason always match the provider
  that answered. OpenAI, Anthropic and Gemini upstream wires are all
  implemented, in both directions.

Deterministic: no wall clock, no unseeded randomness, fixed ids and
timestamps. Every choice is a function of the brief and the answers
seen so far, so a replay leaves the same state export.

## Local practice results

Run against the kit's env and scorer, `practice/trial-0/1/2`:

| trial | score | quality | served | wire | failover | catalog |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 99 | 99 | 100 | 100 | 100 | 100 |
| 1 | 99 | 99 | 100 | 100 | 100 | 100 |
| 2 | 99 | 99 | 100 | 100 | 100 | 100 |

20 of 20 requests valid in each trial, 28–30 upstream calls
(20 served plus the refused calls that were failed over).
