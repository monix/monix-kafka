
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
ConsumerBenchmark.monixAutoCommitHeartbeat10ms      thrpt    7   4.865 ± 1.044  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat1000ms  thrpt    7   2.978 ± 0.006  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat100ms   thrpt    7   9.961 ± 1.317  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat10ms    thrpt    7  13.346 ± 0.716  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat15ms    thrpt    7  13.454 ± 2.680  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat1ms     thrpt    7  14.281 ± 1.591  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat50ms    thrpt    7  11.900 ± 0.698  ops/s
---
### 1 fork 3 threads 
Benchmark                                               Mode  Cnt   Score   Error  Units
ConsumerBenchmark.monixAutoCommitHeartbeat10ms      thrpt    7   4.865 ± 1.044  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat1000ms  thrpt    7   2.978 ± 0.006  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat100ms   thrpt    7   9.961 ± 1.317  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat10ms    thrpt    7  13.346 ± 0.716  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat15ms    thrpt    7  13.454 ± 2.680  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat1ms     thrpt    7  14.281 ± 1.591  ops/s
ConsumerBenchmark.monixManualCommitHeartbeat50ms    thrpt    7  11.900 ± 0.698  ops/s

```sbt
sbt 'benchmarks/jmh:run -i 5 -wi 1 -f1 -t1 monix.kafka.benchmarks.ConsumerBenchmark.*'
```