package com.cloudcomputing.samza.nycabs;

import org.apache.samza.context.Context;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.Map.Entry;
import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

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
    private ObjectMapper objectMapper;
    private double MAX_MONEY = 100.0;


    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        this.driverLocationStore = (KeyValueStore<String, String>) context.getTaskContext().getStore("driver-loc");
        this.objectMapper = new ObjectMapper();
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
        Map<String, Object> message = (Map<String, Object>) envelope.getMessage();
        String messageType = (String) message.get("type");

         if ("ENTERING_BLOCK".equals(messageType) || "LEAVING_BLOCK".equals(messageType)) {
            // Handle driver entering or leaving block
            handleDriverLocationUpdates(message, messageType);
        } else if ("RIDE_REQUEST".equals(messageType)) {
            // Handle ride request
            handleRideRequest(message, collector);
        }
    }

    private void handleDriverLocationUpdates(Map<String, Object> message, String messageType) {
        String driverId = message.get("driverId").toString();
        Integer blockId = (Integer) message.get("blockId");

        if ("ENTERING_BLOCK".equals(messageType)) {
            // Driver is entering a block, so update their location in the store.
            try {
                String driverLocationJson = objectMapper.writeValueAsString(message);
                driverLocationStore.put(driverId, driverLocationJson);
            } catch (JsonProcessingException e) {
                // Handle serialization error
                throw new RuntimeException("Error serializing driver location data", e);
            }
        } else if ("LEAVING_BLOCK".equals(messageType)) {
            // Driver is leaving a block, so remove their location from the store.
            driverLocationStore.delete(driverId);
        }
    }

    private void handleRideRequest(Map<String, Object> message, MessageCollector collector) {
        // Parse the client request message
        Integer clientId = (Integer) message.get("clientId");
        Double clientLatitude = (Double) message.get("latitude");
        Double clientLongitude = (Double) message.get("longitude");
        String genderPreference = (String) message.get("gender_preference");
        Integer blockId = (Integer) message.get("blockId");

        // Retrieve all available drivers in the same block
    List<Map<String, Object>> driversInBlock = getAllDriversInBlock(blockId);
    Map<String, Object> bestMatch = null;
    double highestMatchScore = -1;

    for (Map<String, Object> driverData : driversInBlock) {
        // Get driver details from the driverData map
        double distanceScore = calculateDistanceScore(driverData, clientLatitude, clientLongitude);
        double genderScore = calculateGenderScore(driverData, genderPreference);
        double ratingScore = calculateRatingScore(((Number) driverData.get("rating")).doubleValue());
        double salaryScore = calculateSalaryScore(((Number) driverData.get("salary")).doubleValue());

        double matchScore = distanceScore * 0.4 + genderScore * 0.1 + ratingScore * 0.3 + salaryScore * 0.2;

        if (matchScore > highestMatchScore) {
            highestMatchScore = matchScore;
            bestMatch = driverData;
        }
    }

    if (bestMatch != null) {
        // Construct the match JSON object to send to the match stream
        Map<String, Object> match = new HashMap<>();
        match.put("clientId", clientId);
        match.put("driverId", bestMatch.get("driverId"));
        String matchJson = toJson(match);
        
        // Send the match to the match-stream
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "match-stream"), matchJson));
    }
}


    // Helper methods for calculating scores 
    private double calculateDistanceScore(Map<String, Object> driverData, double clientLatitude, double clientLongitude) {
        double driverLatitude = (Double) driverData.get("latitude");
        double driverLongitude = (Double) driverData.get("longitude");

        // Calculate the Euclidean distance
        double distance = Math.sqrt(Math.pow(driverLatitude - clientLatitude, 2) + Math.pow(driverLongitude - clientLongitude, 2));

        // Calculate the score using the distance
        double distanceScore = Math.exp(-1 * distance);

        return distanceScore;
    }

    private double calculateGenderScore(Map<String, Object> driverData, String genderPreference) {
        String driverGender = (String) driverData.get("gender");

        // If the client has no preference, return 1.0
        if ("N".equals(genderPreference)) {
            return 1.0;
        }

        // If the driver's gender matches the client's preference, return 1.0, otherwise return 0.0
        return driverGender.equals(genderPreference) ? 1.0 : 0.0;
    }

    private double calculateRatingScore(double driverRating) {
        return driverRating / 5.0;
    }

    private double calculateSalaryScore(double driverSalary) {
        return 1 - (driverSalary / 100.0);
    }
    
    /*
    // calculating match score (you will need actual driver details and client preferences)
            double distanceScore = calculateDistanceScore(driverLatitude, driverLongitude, clientLatitude, clientLongitude);
            double genderScore = calculateGenderScore(driverGender, clientGenderPreference);
            double ratingScore = calculateRatingScore(driverRating);
            double salaryScore = calculateSalaryScore(driverSalary);

            double matchScore = distanceScore * 0.4 + genderScore * 0.1 + ratingScore * 0.3 + salaryScore * 0.2;

            // Send match to output stream if a suitable driver is found
            Map<String, Object> match = new HashMap<>();
            match.put("clientId", clientId);
            match.put("driverId", bestMatchingDriverId);

            collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "match-stream"), match));
        }

        //String incomingStream = envelope.getSystemStreamPartition().getStream();

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
    */
    private String toJson(Map<String, Object> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (IOException e) {
            throw new RuntimeException("Error serializing data to JSON", e);
        }
    }

    // Assume a Driver class is defined elsewhere with appropriate fields and methods
    private List<Map<String, Object>> getAllDriversInBlock(Integer blockId) {
        List<Map<String, Object>> driversInBlock = new ArrayList<>();
    
        // Iterate over all entries in the driverLocationStore
        KeyValueIterator<String, String> allDrivers = driverLocationStore.all();
        while (allDrivers.hasNext()) {
            org.apache.samza.storage.kv.Entry<String, String> entry = allDrivers.next(); 
            String driverJson = entry.getValue();
            try {
                // Parse the JSON string to a Map
                Map<String, Object> driverData = objectMapper.readValue(driverJson, new TypeReference<Map<String, Object>>() {});
                // Check if the driver is in the specified block
                if (blockId.equals(driverData.get("blockId"))) {
                    driversInBlock.add(driverData);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error getting all drivers on this BlockID", e);
            }
        }
        allDrivers.close(); // Important to close the iterator to avoid resource leaks
        return driversInBlock;
    }


    private void sendMatchToOutputStream(MessageCollector collector, Integer clientId, Integer driverId) {
        try {
            Map<String, Object> matchMap = new HashMap<>();
            matchMap.put("clientId", clientId);
            matchMap.put("driverId", driverId);

            String matchJson = objectMapper.writeValueAsString(matchMap);
            collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "match-stream"), matchJson));
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error serializing match data to JSON", e);
        }
    }

}
