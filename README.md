# Disruptor Avro Writer
The Disruptor Avro Writer library is an AOL project that provides a specialized, asynchronous version of the Avro writer
in [Apache's Java Avro library](http://avro.apache.org/docs/current/gettingstartedjava.html).

By wrapping Apache's writer with an [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) front-end, our library
allows client code to write Avro objects to disk taking advantage of the high-throughput, low-latency achieved by Disruptor.
 
## How to get it
Add the following Maven dependency to your project:

    <dependency>
      <groupId>com.aol.advertising.dmp</groupId>
      <artifactId>disruptor_avro_writer</artifactId>
      <version>2.4.1</version>
    </dependency>
    
## Overview
The library provides the following:

* Efficient, asynchronous serialization of Avro objects into disk.
* Customizable rolling management of the generated files.

## API
The library contains a DisruptorAvroFileWriter interface with a single method *write* that writes a given Avro record to a file in disk.

```java
public interface DisruptorAvroFileWriter extends AutoCloseable {

  /**
   * Writes an Avro record to file
   */
  void write(final SpecificRecord avroRecord);

}
```

A builder and a factory are provided to obtain instances of DisruptorAvroFileWriter. The target file is specified when
obtaining an instance and the writer will be bound to that file for the rest of its lifecycle.

You shouldn't use multiple writers to write to the same file as that can lead to concurrency problems. Note that writer
instances are thread-safe (with the default configuration, more on this later) and can be safely called from multiple threads,
so the right approach is to have all your threads writing to the same destination file share the same writer singleton.
On the other hand, the builder and the factory are designed to be used only during your application startup/wiring to
provide the necessary writers and are not thread-safe (these design patterns are typically not thread-safe anyway).

The DisruptorAvroFileWriter *close* method should be called when shutting down your application in order to flush any remaining objects into disk.

### Using the builder
The builder API provides a DSL suitable for standalone applications with no dependency injection or for programmatic
configuration styles such as Spring's Java-based configuration. As mentioned earlier, writers have a 1:1 relationship
with the destination files, so when building a new instance you need to specify both the destination file path and the
Avro schema that will be used to serialize the Avro objects. The following code snippet shows an example on how to get a writer instance:

```java
    DisruptorAvroFileWriterBuilder.startCreatingANewWriter()
                                  .thatWritesTo(avroFile)
                                  .thatWritesRecordsOf(avroSchema)
                                  .createNewWriter();

```

Steps *thatWritesTo* and *thatWritesRecordsOf* are mandatory and the API will not offer the *createNewWriter* step until
these have been called.

The builder also contains optional steps to customize the Disruptor used:

* Ring buffer size. Default is 2048 entries:
 
```java
  public OptionalSteps withRingBufferSize(int ringBufferSize);
  
```
 
* Producer type. Default is [ProducerType.MULTI](https://lmax-exchange.github.io/disruptor/docs/com/lmax/disruptor/dsl/ProducerType.html#MULTI)
  
```java
  public OptionalSteps withProducerType(final ProducerType producerType);
  
```
 
  Keep in mind that changing the type from MULTI to SINGLE will improve writer instances performance slightly but will
  render them **non thread-safe**.
  
* Buffer consumer write strategy. Default is [SleepingWaitStrategy](https://lmax-exchange.github.io/disruptor/docs/com/lmax/disruptor/SleepingWaitStrategy.html)

```java
  public OptionalSteps withWaitStrategy(final WaitStrategy waitStrategy);
  
```
   
Finally, the writer can be configured on how to roll the Avro files. By default, a time and size policy is used, similar to
[SizeAndTimeBasedFNATP](http://logback.qos.ch/apidocs/ch/qos/logback/core/rolling/SizeAndTimeBasedFNATP.html) in the
Logback logging library. Time-based rolling will happen every night at midnight. Size-based rolling will happen by
default when a size of 50Mb is reached. This size can be configured using the TimeAndSizeBasedRollingPolicyConfig class:

```java
    final TimeAndSizeBasedRollingPolicyConfig defaultRollingPolicyConfig =
        new TimeAndSizeBasedRollingPolicyConfig().withFileRollingSizeOf(rollSizeInMb);
    return DisruptorAvroFileWriterBuilder.startCreatingANewWriter()
                                         .thatWritesTo(avroFile)
                                         .thatWritesRecordsOf(avroSchema)
                                         .withDefaultRollingPolicyConfiguration(defaultRollingPolicyConfig)
                                         .createNewWriter();

```

You can also fully override the rolling behavior by implementing your own version of the RollingPolicy interface and then passing it to the builder:

```java
    public OptionalSteps withRollingPolicy(final RollingPolicy rollingPolicy);

```

### Using the factory
This API is suitable for applications with dependency injection and declarative configuration styles such as Spring's
XML-based configuration. This API is simply a wrapper around the builder and offers the same operations via settable
bean properties. For example, you could use the factory in a Spring XML config file as follows:

```xml
  <bean id="disruptorAvroFileWriterFactory"
        class="com.aol.advertising.dmp.disruptor.api.DisruptorAvroFileWriterFactory">
    <property name="avroFileName" ref="avroFilePath"/>
    <property name="avroSchema" ref ="avroSchema"/>
    <property name="rollingPolicy" ref="rollingPolicy"/>
  </bean>

  <bean id="disruptorAvroFileWriter" factory-bean="disruptorAvroFileWriterFactory"
        factory-method="createNewWriter"
        destroy-method="close"/>
```
  