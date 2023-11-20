package com.cloudcomputing.samza.nycabs.application;

import com.cloudcomputing.samza.nycabs.DriverMatchTask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;

public class DriverMatchTaskApplication implements TaskApplication {
    // Consider modify this zookeeper address, localhost may not be a good choice.
    // If this task application is executing in slave machine.
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("172.31.4.37.ec2.internal:9092");

    // Consider modify the bootstrap servers address. This example only cover one
    // address.
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("172.31.4.37.ec2.internal:9092","172.31.2.162.ec2.internal:9092","172.31.4.177.ec2.internal:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    // Kafka configuration from properties file
    private static final String SYSTEM_NAME = "kafka";
    private static final String INPUT_STREAM_DRIVER_LOC = "driver-locations";
    private static final String INPUT_STREAM_EVENTS = "events";
    private static final String OUTPUT_STREAM_MATCHES = "match-stream";

    @Override
    public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {
        // Define a system descriptor for Kafka.
        
        //KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(SYSTEM_NAME);
                
        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor("kafka")
                .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        // Hint about streams, please refer to DriverMatchConfig.java
        // We need two input streams "events", "driver-locations", one output stream
        // "match-stream".

        // Define your input and output descriptor in here.
        // Reference solution:
        // https://github.com/apache/samza-hello-samza/blob/master/src/main/java/samza/examples/wikipedia/task/application/WikipediaStatsTaskApplication.java
        KafkaInputDescriptor<JsonSerde> eventsDescriptor = kafkaSystemDescriptor.getInputDescriptor(
            "events", new JsonSerde<>());

        KafkaInputDescriptor<JsonSerde> driverLocationsDescriptor = kafkaSystemDescriptor.getInputDescriptor(
            "driver-locations", new JsonSerde<>());

        KafkaOutputDescriptor<JsonSerde> matchStreamDescriptor = kafkaSystemDescriptor.getOutputDescriptor(
            "match-stream", new JsonSerde<>());

        /*
        KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(SYSTEM_NAME);

        KafkaInputDescriptor driverLocationsDescriptor = kafkaSystemDescriptor.getInputDescriptor(
                INPUT_STREAM_DRIVER_LOC, new JsonSerde<>());
        KafkaInputDescriptor eventsDescriptor = kafkaSystemDescriptor.getInputDescriptor(
                INPUT_STREAM_EVENTS, new JsonSerde<>());
        KafkaOutputDescriptor matchesDescriptor = kafkaSystemDescriptor.getOutputDescriptor(
                OUTPUT_STREAM_MATCHES, new JsonSerde<>());
        */
        
        // Bound you descriptor with your taskApplicationDescriptor in here.
        // Please refer to the same link.

        //taskApplicationDescriptor.withTaskFactory((StreamTaskFactory)() -> new DriverMatchTask());

        taskApplicationDescriptor
        .withInputStream(eventsDescriptor)
        .withInputStream(driverLocationsDescriptor)
        .withOutputStream(matchStreamDescriptor)
        .withTaskFactory((StreamTaskFactory) () -> new DriverMatchTask());
    }
}
