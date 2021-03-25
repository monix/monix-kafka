
## RC1

Benchmark                                    Mode  Cnt   Score    Error  Units
ConsumerBenchmark.auto_commit_async_1P_1RF  thrpt   10  53.286 ± 15.924  ops/s
ConsumerBenchmark.auto_commit_async_2P_1RF  thrpt   10  45.976 ± 17.338  ops/s
ConsumerBenchmark.auto_commit_sync_1P_1RF   thrpt   10  52.235 ± 12.441  ops/s
ConsumerBenchmark.auto_commit_sync_2P_1RF   thrpt   10  49.599 ± 12.737  ops/s
ConsumerBenchmark.manual_commit_1P_1RF      thrpt   10  40.858 ± 10.888  ops/s
ConsumerBenchmark.manual_commit_2P_1RF      thrpt   10  44.895 ± 10.388  ops/s

## RC2 - Introduces PollHeartbeatInterval

ConsumerBenchmark.monix_auto_commit                  thrpt   10  10.569 ± 2.096  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1     thrpt   10  10.320 ± 1.720  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat100   thrpt   10   4.518 ± 0.266  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1000  thrpt   10   0.994 ± 0.002  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat3000  thrpt   10   0.332 ± 0.001  ops/s