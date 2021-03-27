
## RC7

### 1fork 1thread
Benchmark                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monix_auto_commit    thrpt   10  11.912 ± 0.617  ops/s
ConsumerBenchmark.monix_manual_commit  thrpt   10  11.519 ± 2.247  ops/s

### 1 fork 3 threads
Benchmark                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monix_auto_commit    thrpt   10  16.186 ± 0.920  ops/s
ConsumerBenchmark.monix_manual_commit  thrpt   10  16.319 ± 1.465  ops/s


## RC8 - (Introduces PollHeartbeatRate)
### 1fork 1thread
---
Benchmark                                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monix_auto_commit10ms                thrpt   10  11.090 ± 1.883  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1000ms  thrpt   10   0.993 ± 0.002  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat100ms   thrpt   10   4.792 ± 0.017  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat10ms    thrpt   10   8.249 ± 0.305  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1ms     thrpt   10  10.038 ± 0.433  ops/s
---
### 1 fork 3 threads 
Benchmark                                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monix_auto_commit10ms                thrpt   10  17.266 ± 2.231  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1000ms  thrpt   10   2.971 ± 0.009  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat100ms   thrpt   10   9.477 ± 0.064  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat10ms    thrpt   10  14.710 ± 1.660  ops/s
ConsumerBenchmark.monix_manual_commit_heartbeat1ms     thrpt   10  15.494 ± 4.163  ops/s
