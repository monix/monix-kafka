
## RC7
Benchmark                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monix_auto_commit    thrpt       11.876          ops/s
ConsumerBenchmark.monix_manual_commit  thrpt       11.964          ops/s


## RC8 - (Introduces PollHeartbeatRate)

1fork 1thread
---
ConsumerBenchmark.monix_auto_commit                  thrpt   10  10.569 ± 2.096  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1     thrpt   10  10.320 ± 1.720  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat100   thrpt   10   4.518 ± 0.266  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1000  thrpt   10   0.994 ± 0.002  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat3000  thrpt   10   0.332 ± 0.001  ops/s
---
1fork 3thrads 
Benchmark                                             Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monix_auto_commit                  thrpt   10  16.270 ± 3.339  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1     thrpt   10  15.053 ± 0.959  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat100   thrpt   10   9.525 ± 1.131  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1000  thrpt   10   2.968 ± 0.010  ops/s




