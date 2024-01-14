<div id="top"></div>
<p align="center">
<img src="docs/images/kafkathena_logo.png" width="250" alt="Kafkathena"/>
</p>

<h1 align="center">Smart, Fast, Customizable Consumer Configurations</h1>

<p align="center">
<a href="https://github.com/Trendyol/kafkathena-commons/blob/next/LICENSE">
    <img src="https://img.shields.io/github/v/release/Trendyol/kafkathena-commons" alt="Release" />
  </a>
<a href="https://img.shields.io/badge/spring%20boot-2.x%7C3.x-orange">
    <img src="https://img.shields.io/badge/spring%20boot-2.x%7C3.x-orange" alt="License" />
  </a>
  <a href="https://github.com/Trendyol/kafkathena-commons/blob/next/LICENSE">
    <img src="https://img.shields.io/github/license/trendyol/baklava" alt="Spring Boot Version" />
  </a>
</p>

<!-- ABOUT THE PROJECT -->
## About The Project
Kafkathena common utilities project.

<!-- Features -->
## Features

* EnableKafkathena annotation
    * This will import classes into the parent project
* Consumer Factory creating function
* Consumer Listener Factory creating function
* Failover handler bean interface
* Producer creating function
* Use all defined producer with KSSender
* Filter Strategy Utility

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- Build With -->
### Built With

This section should list any major frameworks/libraries used to bootstrap your project. Leave any add-ons/plugins for the acknowledgements section. Here are a few examples.

* [Spring Starter 3+]
* [Spring Kafka Starter]
* [Jdk 17]

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- GETTING STARTED -->
## Getting Started

This is an example of how you may give instructions on setting up your project locally.
To get a local copy up and running follow these simple example steps.

### Prerequisites

This is an example of how to list things you need to use the software and how to install them.
* Maven 3+
* Jdk 17

### Installation
1. Copy and paste this inside your pom.xml dependencies block.
```xml
<dependency>
  <groupId>com.trendyol.mpc</groupId>
  <artifactId>kafkathena-commons</artifactId>
  <version>RELEASE</version>
</dependency>
```
2. Registry setup. If you haven't already done so, you will need to add the below to your pom.xml file.
```xml
<repositories>
  <repository>
    <id>gitlab-maven</id>
    <url>https://gitlab.trendyol.com/api/v4/projects/4093/packages/maven</url>
  </repository>
</repositories>

<distributionManagement>
  <repository>
    <id>gitlab-maven</id>
    <url>https://gitlab.trendyol.com/api/v4/projects/4093/packages/maven</url>
  </repository>

  <snapshotRepository>
    <id>gitlab-maven</id>
    <url>https://gitlab.trendyol.com/api/v4/projects/4093/packages/maven</url>
  </snapshotRepository>
</distributionManagement>
```

<p align="right">(<a href="#top">back to top</a>)</p>

<!-- USAGE EXAMPLES -->
## Usage

1. Implement Consumer Configurations
2. Implement Producer Configurations
3. Define your listeners

```
kafkathena:
  shared-factory-props:
    autoStartup: true
    missingTopicAlertEnable: false
    concurrency: 1
    syncCommitTimeoutSecond: 5
    syncCommit: true
    ackMode: RECORD
    interceptorClassPath: com.trendyol.kafkathena.demo.interceptor.KafkaConsumerInterceptor
  producers:
    default:
      props:
        "[bootstrap.servers]": ${KAFKA_BOOTSTRAP_SERVERS:localhost:29092}
        "[batch.size]": 16384
        "[linger.ms]": 0
        "[buffer.memory]": 33554432
        "[key.serializer]": org.apache.kafka.common.serialization.StringSerializer
        "[value.serializer]": org.springframework.kafka.support.serializer.JsonSerializer
        "[acks]": "1"
        "[request.timeout.ms]": 30000
  consumers:
    "[consumer-one]":
      type: JSON # AVRO/PROTO/JSON it can be empty
      topic: kafkathena.topic.one
      factory-bean-name: consumerOneKafkaListenerContainerFactory
      data-class: com.trendyol.kafkathena.demo.model.ConsumerOneMessage
      error-producer-name: default
      filter-header:
        error-producer-filter-key: one-filter
        consumer-filter-key: one-filter
        ignored-exception-classes: 
            - class 1
      failover:
        error-topic: kafkathena.topic.error
        handler-bean-name: defaultConsumerFailoverHandler
      fixed-retry:
        retry-count: 1
        backoff-interval-millis: : 5000 #wait time for retry
      exponential-retry:
        retry-count: : 1
        multiplier: 2
        maxInterval: 5
        backoff-interval-millis: : 1000
      factory-props:
        auto-startup: : true
        missing-topic-alert-enable: : false
        concurrency: 1
        sync-commit-timeout-second: : 5
        sync-commit: : true
        ack-mode: : RECORD
        interceptor-class-path: : com.trendyol.kafkathena.demo.interceptor.KafkaConsumerInterceptor
      props:
        "[bootstrap.servers]": ${KAFKA_BOOTSTRAP_SERVERS:localhost:29092}
        "[group.id]": kafkathena.topicOneGroup
        "[value.deserializer]": org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
        "[spring.deserializer.value.delegate.class]": org.springframework.kafka.support.serializer.JsonDeserializer
        "[key.deserializer]": org.springframework.kafka.support.serializer.ErrorHandlingDeserializer
        "[spring.deserializer.key.delegate.class]": org.apache.kafka.common.serialization.StringDeserializer
        "[max.poll.records]": 100
        "[max.poll.interval.ms]": 300000
        "[session.timeout.ms]": 300000
        "[heartbeat.interval.ms]": 3000
        "[enable.auto.commit]": true
        "[auto.offset.reset]": earliest
        "[fetch.max.bytes]": 52428800
        "[fetch.max.wait.ms]": 500
```
```
@Component
@DependsOnKafkathena
public class ConsumerOne {

    @KafkaListener(
            topics = "${kafkathena.consumers[consumer-one].topic}",
            groupId = "${kafkathena.consumers[consumer-one].props[group.id]}",
            containerFactory = "${kafkathena.consumers[consumer-one].factory-bean-name}"
    )
    public void consume(@Payload ConsumerOneMessage message) {

    }
}
```

License
--------

    MIT License

    Copyright (c) 2022 Trendyol
    
    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:
    
    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.
    
    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.

<p align="right">(<a href="#top">back to top</a>)</p>
