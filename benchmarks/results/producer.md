
## RC7

### 10iterations 1fork 1thread
Benchmark                                 Mode  Cnt  Score   Error  Units
ProducerBenchmark.monix_single_producer  thrpt    3  0.504 ± 0.668  ops/s
ProducerBenchmark.monix_sink_producer    thrpt    3  1.861 ± 5.475  ops/s

## RC8 

### 10iterations 1fork 3threads 
Benchmark                                 Mode  Cnt  Score   Error  Units
ProducerBenchmark.monix_single_producer  thrpt   10  0.981 ± 0.202  ops/s
ProducerBenchmark.monix_sink_producer    thrpt   10  3.241 ± 0.191  ops/s
