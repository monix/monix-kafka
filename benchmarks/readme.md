###Monix Kafka Benchmarks

This document explains the approach followed to benchmark monix-kafka.

Although it would be nice to summit monix-kafka onto soak, stress or load tests, we have hardware limitations that makes no possible to run those.

Therefore we will stick to run performance testing on the different producer and consumer implementations that monix-kafka is exposing.

Being `KafkaProducer`, `KafkaProducerSink` and `KafkaConsumerObservable` (AutoCommit and Committable) and finally doing both producer and consumer.

Kafka is a distributed publisher/subscriber paradigm that have lots of configurables whose can directly impact on the performance of the 
 application running kafka (such as the number of partitions, replication factor, poll interval, records, buffer sizes and more...). 
 
 For these performance tests we won't focus that much on these specific kafka configurations since these will run inside 
 a local virtual environment, which would not really reflect the real latency spent on the communication within the kafka cluster (network latency, lost packages, failures...), thus, we 
  do focus more on specific monix configuration (parallelism, async vs sync and commit order). 
  
Single `KafkaProducer` and `KafkaProducerSink` benchmark (they are different benchmarks but configuration is shared): 

 | _Parallelism / Partitions, Replication Factor_  | __1P__, __1RF__ | __1P__, __2RF__ | __2P__, __2RF__ | 
  | :---: | :---: | :---: | :---: |
  | __1__| ...| ... | ... |
  | __2__| ... | ...| ... |
  | __5__ | ... | ... | ... |
  | __10__ | ... | ... | ... |
  
Single `KafkaConsumer` (Auto commit and Manual commit) (consider including commit order)

 | _Type / Partitions, Replication Factor_  | __1P__, __1RF__ | __1P__, __2RF__ | __2P__, __2RF__ | 
  | :---: | :---: | :---: | :---: |
  | __async__| ...| ... | ... |
  | __sync__| ... | ...| ... |

Pair `KafkaProducerSink` and `KafkaConsumer`: This test will consist in an observable being consumed by a kafka producer sink that produces 
to an specific topic and then it gets consumed by a kafka consumer one. 