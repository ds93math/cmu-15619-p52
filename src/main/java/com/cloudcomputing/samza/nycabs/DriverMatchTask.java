package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask {

    /* Define per task state here. (kv stores etc)
       READ Samza API part in Writeup to understand how to start
    */
    private KeyValueStore<String, String> driverLocationStore;
    private KeyValueStore<String, String> clientRequestStore;
    private double MAX_MONEY = 100.0;

    private JSONUtil jsonUtil;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        this.driverLocationStore = (KeyValueStore<String, String>) context.getTaskContext().getStore("driver-location-store");
        this.clientRequestStore = (KeyValueStore<String, String>) context.getTaskContext().getStore("client-request-store");
        this.jsonUtil = new JSONUtil();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        /*
        All the messsages are partitioned by blockId, which means the messages
        sharing the same blockId will arrive at the same task, similar to the
        approach that MapReduce sends all the key value pairs with the same key
        into the same reducer.
        */
        String incomingStream = envelope.getSystemStreamPartition().getStream();

        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
	    // Handle Driver Location messages
            DriverLocation location = parseDriverLocation(envelope.getMessage());
            driverLocationStore.put(location.getDriverId(), location);
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
            // Parse the client request message, calculate match scores, and find the best match
            ClientRequest request = parseClientRequest(envelope.getMessage());
            List<DriverLocation> driversInTheSameBlock = driverLocationStore.getAll().stream()
                .filter(driver -> driver.getBlockId().equals(request.getBlockId()))
                .collect(Collectors.toList());
            
            // Then construct the output message and send it to the match-stream
            DriverLocation bestMatchDriver = findBestMatchDriver(request, driversInTheSameBlock);
            if (bestMatchDriver != null) {
                sendMatchToOutputStream(collector, request.getClientId(), bestMatchDriver.getDriverId());
            }
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    private void sendMatchToOutputStream(MessageCollector collector, Integer clientId, Integer driverId) {
        Map<String, Object> matchMap = new HashMap<>();
        matchMap.put("clientId", clientId);
        matchMap.put("driverId", driverId);

        String matchJson = jsonUtil.toJson(matchMap);
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "match-stream"), matchJson));
    }

}
