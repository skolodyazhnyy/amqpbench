amqpbench
=========

> _It's like Apache Benchmark (ab) but for AMQP server_

amqpbench allows to publish multiple messages to your AMQP server to test its performance.

Usage
-----

For example, to publish `5000` messages using `100` concurrent requests, simply run
```
./amqpbench -amqp-url "amqp://guest:guest@myamqp.server/" -c 100 -n 5000
```

The output will give you some basic statistics

```
Sending messages (be patient)...
1000 messages are published
2000 messages are published
3000 messages are published
4000 messages are published
5000 messages are published

Concurrent level:       100
Time taken for tests:   16.625284001s
Published messages:     5000
Failed messages:        0
Messages per second:    3.07 (mean)
Time per message:       325.609565ms (mean)
Time per message:       3.307197ms (mean, across all concurrent publishers)

Percentage of the messages published within a certain time
    50%         310.234466ms
    66%         349.777646ms
    75%         354.806416ms
    80%         387.569834ms
    90%         399.885463ms
    95%         484.003913ms
    98%         706.338643ms
    99%         941.922161ms
    100%        1.04787277s
```
