# Monix Kafka 

<img src="/website/static/img/kafka.png" width="125" align="right">


[![Build Status](https://travis-ci.org/monix/monix-kafka.svg?branch=master)](https://travis-ci.org/monix/monix-kafka)
[![Maven Central](https://img.shields.io/maven-central/v/io.monix/monix-kafka-1x_2.12.svg)](https://search.maven.org/search?q=g:io.monix%20AND%20a:monix-kafka-1x_2.12)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-brightgreen.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/monix/monix?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

See the [__documentation web site__](https://monix.github.io/monix-kafka) to get started.

### Caveats

[Issue#101](https://github.com/monix/monix-kafka/issues/101)
Starting from Kafka 0.10.1.0, there is `max.poll.interval.ms` setting:

    The maximum delay between invocations of poll() when using consumer group management. 
    This places an upper bound on the amount of time that the consumer can be idle before 
    fetching more records. If poll() is not called before expiration of this timeout, 
    then the consumer is considered failed and  the group will rebalance in order 
    to reassign the partitions to another member.

Since, monix-kafka backpressures until all records has been processed. 
This could be a problem if processing takes time.
You can reduce `max.poll.records` if you are experiencing this issue.

## How can I contribute to Monix-Kafka?

We welcome contributions to all projects in the Monix organization and would love
for you to help build Monix-Kafka. See our [contributor guide](./CONTRIBUTING.md) for
more information about how you can get involed.

## Maintainers

The current maintainers (people who can merge pull requests) are:

- Alexandru Nedelcu ([alexandru](https://github.com/alexandru))
- Alex Gryzlov ([clayrat](https://github.com/clayrat))
- Piotr Gawryś ([Avasil](https://github.com/Avasil))
- Leandro Bolivar ([leandrob13](https://github.com/leandrob13))

## License

All code in this repository is licensed under the Apache License,
Version 2.0.  See [LICENSE.txt](./LICENSE).
