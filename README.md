## kafka-codec: A Netty codec for Apache Kafka

Netty is a great framework for creating network applications. Netty has an elegant 
threading model in which all I/O and event handling is done by the thread associated 
with Netty’s event loop. This enables you to write simple, concurrency free, single 
threaded code that could then monopolize a CPU core and max out its performance.

Kafka is a great message broker. Kafka ships with a Java client that can be used to 
produce messages to and consume messages from a Kafka cluster. But the Java client 
provided by Kafka creates threads of its own and will not work with Netty’s event 
loop. So if your Netty application needs to speak with a Kafka cluster, and you 
choose to use the Java client provided by Kafka, you end up with multiple threads 
of execution. In other words, you are forced to write multithreaded software.

What is provided here is a codec that can be used by a Netty application to directly 
speak with a Kafka cluster. This codec works within the constraints of Netty’s event 
loops restoring the elegant threading model that Netty provides.
