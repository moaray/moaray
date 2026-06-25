# moaray

> Open-source **Mixture-of-Agents (MoA) gateway**, written in Rust.

A dual-mode AI gateway:

- **Passthrough mode** — a thin, fast OpenAI-compatible gateway (route / forward / rate-limit). Sub-millisecond overhead target.
- **MoA mode** — fan-out to multiple models in parallel, then aggregate / fuse / quorum-judge the results into a single higher-quality answer.

One Rust binary, two paths, split by the request's `model` field:

```
model: "gpt-5.5"      -> passthrough (traditional gateway)
model: "moa/<recipe>" -> orchestration (fan-out + aggregate)
```

## Why

Today's open-source LLM gateways all do routing + failover. None do **parallel fan-out + aggregation + quality judging**. `moaray` fills that gap — drop-in replace your gateway, and unlock a quality-boosting MoA mode on top.

## Status

🚧 Early. Spec & MVP in progress.

## License

TBD
