###Monix Kafka Benchmarks

This document explains the approach followed to benchmark monix-kafka.

 Ideally, a Kafka performance benchmark should happen under some long stress test in a real Kafka cluster,
although, our hardware limitations we have to stick to running simpler basic tests that proves application performance on 
 a docker container.



So these test will be executed using the same configurations ()

The benchmark will focus on the most basic `consumer` and `procucer` scenarios. 

Although Kafka is very configurable by nature, our benchmark will use the kafka default properties for [consumer](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
and [producer](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html).

On the other hand, assume that all the used topics will have 2 partitions, and 1 replication factor.
  

## Producer benchmarks

This section includes benchmarks for single and sink producers. 
Although some libraries like `alpakka-kafka` do not expose methods for producing single record, but only for sink.


## Consumer benchmark
The consumer benchmark covers the manual and auto commit consumer implementations of the different libraries. 
The manual commit will also cover producing committing back the consumed offsets.