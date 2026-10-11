# PR qualification

This consolidation retains the original dependency PR histories #2–#5 and the current default branch. It updates the root crypto/net/text graph, the independent Mongo example to the maintained MongoDB v2 driver and Kafka client, and CI/example build toolchains to Go 1.26.9.

Production regressions repaired:
- Zero controller request timeouts now use the client's timeout policy with a two-second default. Explicit request timeouts and request payloads are preserved.
- Error log messages containing percent signs are logged literally.
- DescribeGroups aggregation returns deadline, cancellation, or transport errors instead of asserting them as successful responses and panicking.
- Fetch broker-error and empty-watermark responses are fully consumed before a retry, preserving correlation framing and the unchanged failed-fetch offset.
- A zero-length connection read returns zero bytes without making a broker request.

Acceptance covers the root and independent examples/consumer-mongo-db module. The full root race suite and the example's real Kafka-to-Mongo insert/query/count and invalid-URI controls run on isolated Kafka 3.9.1 and the existing CI Kafka 2.3.1 image. Both use synthetic PLAIN/SCRAM credentials and ephemeral data on an internal network, without host ports. Fixture containers and networks are removed after each run.

Test fixtures now confirm actual partition-leader fetch readiness after topic creation, require acknowledgements for finite data, wait for the tested consumer-generation transition, close their own readers, and bound waits. The stream adapter buffers complete Kafka messages, uses a distinct close key, and retries broker fetch timeouts as the existing production reader does. Its write-deadline simulation remains the existing test-adapter policy; it does not prove physical socket backpressure. Separate real Conn deadline tests remain enabled.

Regression controls reproduce the error-aggregation panic and fetch-frame corruption before their fixes. The fetch control uses actual encoded requests/responses over net.Pipe and verifies successful message delivery after error responses for versions 2, 5 and 10, including record bytes accompanying an empty watermark. Controller controls check default, client-bound and explicit timeouts with unchanged payloads; logging controls check percent-containing errors, ordinary errors and successful commits.

Qualification results and source scan findings are recorded below after final acceptance. The two existing explicitly skipped root cases (truncated-message reproduction and missing-topic behavior with auto-creation) remain disabled. Existing broker-version gates remain; no new test skips or advisory suppressions are added. The private acceptance never sets KAFKA_SKIP_NETTEST. The nine historical CircleCI broker jobs are preserved, but only 2.3.1 and 3.9.1 were exercised locally. No deployment occurred.


## Completed qualification

Root module: the full Kafka 3.9.1 race run passed (144 seconds); after the test-only finite partition-reader bound changed from 100 ms to five seconds, all modern Writer cases passed again (22 seconds). Every other tested modern source file is identical. The full final-source Kafka 2.3.1 race run passed (142 seconds). Both use Go 1.26.9, readonly modules, race detection, count=1 and a 15-minute package deadline.

The independent Mongo module passed real Kafka-to-Mongo race acceptance on both brokers (22 seconds each). Its sources and module graph are unchanged from those receipts; it imports the maintained upstream Kafka client rather than the local fork.

Both modules pass module verification, vet and all-package build. Final source govulncheck reports 0 called and 0 imported vulnerabilities; GO-2026-5932 is present only as an unused required-module finding in each graph. No suppression or policy waiver is used. Source hashes were compared against the accepted immutable contexts and the committed files.
