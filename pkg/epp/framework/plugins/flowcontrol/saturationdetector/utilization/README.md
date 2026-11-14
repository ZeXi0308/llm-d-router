# Utilization Detector Plugin

**Type:** `utilization-detector`

Reactive saturation detection and scheduling filter based on telemetry from LLM serving backends.

> [!NOTE]
> This plugin is enabled by default when flow control is enabled. You do not need to explicitly declare it in your configuration.

## What it does

This plugin uses a two-tier approach to manage average pool load and protect individual endpoints.

### Role in Flow Control (The Gatekeeper)
The detector implements the `SaturationDetector` interface to provide a utilization gradient, allowing the Flow Controller to apply proportional backpressure when the system is overloaded.

It relies on a "roofline model", evaluating both the queue depth and the KV cache utilization to find the most constrained resource for a given endpoint:

    EndpointScore = max(QueueDepth / QueueThreshold, KVCacheUsage / KVCacheThreshold)

With `kvCacheGrowthReserveTokens` set, the score is the larger of this value and `ReserveScore` (see [KV cache growth reserve](#kv-cache-growth-reserve)).

The global pool saturation is the average of the endpoint scores:

    PoolSaturation = Average(EndpointScore)

**Heterogeneous Deployments:** Because this detector calculates saturation as an unweighted average of individual endpoint scores, it treats all endpoints equally regardless of their physical capacity. In deployments with heterogeneous compute (e.g., mixing H100 and L4 nodes), a small, saturated endpoint has the exact same impact on global backpressure as a massive, saturated endpoint. Contrast this with the Concurrency Detector, which evaluates saturation as a single aggregate fraction, biasing toward larger endpoints.
*Note: Endpoints with missing or stale metrics are scored according to `stalenessPolicy`: as 100% saturated under `saturated` (default), or excluded from the pool average under `ignore`. An empty candidate list reads 1.0 under both policies.*

**Operational dependency:** under `saturated`, a fleet-wide scrape outage pins
`flow_control_pool_saturation` at 1.0 and halts dispatch entirely. The `flow_control_stale_endpoints` gauge and a
rate-limited detector log distinguish this from genuine overload: stale endpoints read exactly 1.0, while genuine
oversubscription typically reads above 1.0. `saturated` is the default because admitting blind on missing
data risks overloading model servers with no backpressure signal at all. Staleness also tends to correlate with
overload: a server too busy to serve its metrics endpoint is often the one that is saturated, so `ignore`
discounts the endpoints most likely to be overloaded from the backpressure signal. Deployments whose availability budget cannot absorb a dispatch halt
during a metrics collection outage can select `ignore`, which excludes stale endpoints from the saturation
average and scores an all-stale pool at 0.0, consistent with the Filter's fail-open fallback. Under `ignore`,
a fleet-wide scrape outage means dispatching with no backpressure signal at all; the stale-endpoints gauge and
detector log are the primary operational signal in that posture.

`stalenessPolicy` governs the `SaturationDetector` role only. The `Filter` role always drops stale candidates
and falls back to the original list when every candidate is stale, independent of this setting. Slow scrape
cadences should be tuned with `metricsStalenessThreshold` rather than `ignore`: if the poll interval is
longer than the staleness threshold, every sample reads stale between polls and `saturated` will block
dispatch almost continuously. The policy field is an enum so additional degradations (for example, holding
the last known score for a bounded window) can be added later.

**Scrape-lag compensation:** `WaitingQueueSize` is scraped on a poller while the Flow Controller's dispatch loop runs far faster, so between two scrapes the queue term is stale-low and the gate would let the controller over-dispatch. When an `inflight-load-producer` is configured, the queue term is corrected by the in-flight requests the scrape does not yet reflect:

    Credit = max(0, InFlightRequests - (WaitingQueueSize + RunningRequestsSize))
    QueueScore = (WaitingQueueSize + Credit) / QueueThreshold

`InFlightRequests` is the producer's per-endpoint counter, incremented at dispatch and decremented on request completion, so the correction already accounts for requests that returned since the last scrape. Endpoints without the attribute contribute zero credit.

The correction rests on these assumptions:

- **`RunningRequestsSize` must be populated.** The subtraction of running requests is only sound when the metrics mapping defines `runningRequestsSpec` (defaults exist for vLLM and SGLang). A custom mapping without it leaves `RunningRequestsSize` at 0, and the credit over-counts by the running count.
- **In-flight view across EPP replicas.** `WaitingQueueSize + RunningRequestsSize` is engine-global. When a cross-replica syncer is configured and the producer's `syncCrossReplicaState` is true (the default), `InFlightRequests` is the sum over all EPP replicas, so the correction holds with multiple replicas. Without a syncer, or with `syncCrossReplicaState: false`, each replica sees only its own requests, the credit floors at 0, and the compensation is lost.

#### KV cache growth reserve

A request admitted below `kvCacheUtilThreshold` keeps allocating KV cache while it decodes, and the total growth scales with the number of requests on the endpoint. A fixed threshold leaves the same space above it at any request count. When the cache fills, the engine preempts running sequences and recomputes them later, which a streaming client sees as a stalled response.

`kvCacheGrowthReserveTokens` holds back that many tokens of cache for every request on the endpoint, plus one reserve for the request the gate is about to admit:

    Requests = RunningRequestsSize + WaitingQueueSize + Credit
    CacheTokens = CacheBlockSize * CacheNumBlocks
    ReserveScore = KVCacheUsage + (BookedTokens + (Requests + 1) * Reserve) / CacheTokens

`BookedTokens` is the token count of the `InFlightLoad` attribute: the uncached prompt tokens of requests that have not returned their first response chunk. It covers prompts the engine has not yet written to the cache. Without the attribute it is 0 and `Requests` is the scraped count.

The reserve is skipped for an endpoint that:

- **Does not report its cache size.** `CacheBlockSize` and `CacheNumBlocks` come from the engine's cache configuration metric. Metrics mappings without it leave both at 0, and the endpoint is scored by the threshold terms alone.
- **Has no requests.** An idle endpoint is the best placement the pool can offer, so a reserve larger than the cache does not block it.

The reserve rests on these assumptions:

- **The value fits the workload.** The reserve is the cache a request is expected to still need, so it depends on the output-length distribution. A value that is too small lets preemptions return gradually; one that is too large leaves cache unused and moves waiting time into the flow control queue.
- **`addEstimatedOutputTokens` is disabled on the producer.** With it enabled, `BookedTokens` includes an output estimate held until the stream ends, and the reserve counts output a second time.
- **Responses stream.** The producer releases a request's tokens at its first response chunk. Between the engine allocating the prompt and that chunk, the prompt is in both `KVCacheUsage` and `BookedTokens`, so the score reads high by the prompt's share of the cache. For a streaming response this lasts for the prefill. For a non-streaming response it lasts until the response completes.
- **One engine per Pod.** For a Pod that exposes one metric series per engine, the request counts are summed over the engines while the cache size is that of one engine, so the score reads high by about the engine count.
- **The endpoint decodes.** A prefill-only endpoint holds no decode growth, so the reserve over-reserves there. The `stages` map of `max-saturation-detector` scopes the `Saturation` role of a detector to the decode stage. It does not scope the `Filter` role, so a detector with a reserve should be left out of the prefill scheduling profile.

### Role in Scheduling (The Traffic Shaper)
The detector implements the `Filter` interface to protect individual endpoints. It removes endpoints from candidate lists if their telemetry is stale, or if they exceed specific safety limits:

    MaxQueueLimit = QueueThreshold * (1 + Headroom)
    MaxKVCacheLimit = min(1.0, KVCacheThreshold * (1 + Headroom))

The queue depth compared against `MaxQueueLimit` includes the scrape-lag credit.

With `kvCacheGrowthReserveTokens` set, the filter also removes endpoints whose `ReserveScore` is 1.0 or more. `Headroom` does not relax this test, because its limit is the physical cache size.

This approach allows the Flow Controller to manage average pool load, while the Scheduler retains the flexibility to burst above ideal targets (the "Headroom") to satisfy affinity or scoring objectives.

**Fail-Open Fallback:** To prevent complete routing failure, if *all* candidate endpoints are filtered out (i.e., the entire cluster is over the safety limits or stale), the filter softens and returns the original list of endpoints, allowing the scheduler's scorers to pick the least-bad option.

## Inputs consumed

The plugin consumes standard metrics from endpoints:
- `WaitingQueueSize` (Queue depth metric).
- `RunningRequestsSize` (Running requests metric, used by the scrape-lag credit and the KV cache growth reserve).
- `KVCacheUsagePercent` (KV cache utilization metric).
- `CacheBlockSize` and `CacheNumBlocks` (KV cache size, used by the KV cache growth reserve).
- `UpdateTime` (Timestamp used to calculate metric staleness).

It optionally consumes the `InFlightLoad` endpoint attribute of an `inflight-load-producer` for the scrape-lag credit and the booked tokens of the KV cache growth reserve.

## Configuration

The plugin accepts JSON parameters decoding to the following fields:

- `queueDepthThreshold` (`int`): Target waiting queue depth limit. Serves as the "ideal" queue capacity for a single endpoint. Must be > 0. (Default: `5`)
- `kvCacheUtilThreshold` (`float64`): Target KV cache memory utilization limit, expressed as a fraction. Must be in `(0.0, 1.0]`. (Default: `0.8`)
- `metricsStalenessThreshold` (`string` / duration): Maximum age of metrics before an endpoint is considered stale. How stale endpoints are scored is controlled by `stalenessPolicy`. Must be > 0. (Default: `"200ms"`)
- `stalenessPolicy` (`string`): How endpoints with missing or stale metrics contribute to pool saturation: `"saturated"` scores them as fully saturated; `"ignore"` excludes them from the saturation average. (Default: `"saturated"`)
- `headroom` (`float64`): Allowed burst capacity above the ideal thresholds, expressed as a fraction (e.g., `0.2` for 20%). Must be >= 0.0. (Default: `0.0`)
- `kvCacheGrowthReserveTokens` (`int`): KV cache space, in tokens, reserved for the decode growth of each request on an endpoint. `0` disables the reserve. Must be >= 0. (Default: `0`)
- `inFlightLoadProducerName` (`string`): Name of the `inflight-load-producer` whose `InFlightLoad` attribute supplies the scrape-lag credit and the booked tokens of the KV cache growth reserve. Empty selects the default producer. (Default: `""`)

## Trade-offs

Unlike the Concurrency Detector, this approach operates as a closed-loop controller. It is immune to estimation divergence and reflects the actual performance limits of the continuous batching engine's memory manager. However, it suffers from two fundamental flaws of reactive systems:

1. **Telemetry Staleness (The Thundering Herd)**: Because it relies on asynchronous polling, the view of the endpoint state is perpetually delayed. A sudden burst of traffic can create a severe "thundering herd" condition, where the scheduler routes massive request volumes to a seemingly healthy endpoint before the next metric interval reveals it is completely saturated.
2. **Reactive Backpressure**: By definition, this detector only signals saturation after the inference engine is already under physical duress. It cannot preemptively shield an endpoint from an initial queue buildup; it can only throttle traffic after the physical limits have been breached and latency (TTFT/TPOT) has already degraded. The KV cache growth reserve covers the KV cache part of this limit for endpoints that report their cache size. It does not cover requests the Flow Controller has dispatched that have not yet reached the producer's counters, so a burst dispatched within one scrape interval can still overshoot.
