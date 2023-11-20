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
import java.io.IOException;

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
    private KeyValueStore<String, String> clientRequestStore;
    private ObjectMapper objectMapper;
    private double MAX_MONEY = 100.0;

    //private JSONUtil jsonUtil;

    @Override
    @SuppressWarnings("unchecked")
    public void init(Context context) throws Exception {
        this.driverLocationStore = (KeyValueStore<String, String>) context.getTaskContext().getStore("driver-location-store");
        this.clientRequestStore = (KeyValueStore<String, String>) context.getTaskContext().getStore("client-request-store");
        this.driverLocationStore = (KeyValueStore<String, String>) context.getTaskContext().getStore("driver-loc");
//        this.jsonUtil = new JSONUtil();
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
        // Logic for handling driver location updates <<<<<<<!!!!!!!!!!!!!!
    }

    private void handleRideRequest(Map<String, Object> message, MessageCollector collector) {
        // Parse the client request message
        Integer clientId = (Integer) message.get("clientId");
        Double clientLatitude = (Double) message.get("latitude");
        Double clientLongitude = (Double) message.get("longitude");
        String genderPreference = (String) message.get("gender_preference");
        Integer blockId = (Integer) message.get("blockId");

        // Retrieve all available drivers in the same block
        // Assuming `getAllDriversInBlock` is a method to retrieve drivers
        List<Driver> driversInBlock = getAllDriversInBlock(blockId);
        Driver bestMatch = null;
        double highestMatchScore = -1;

        for (Driver driver : driversInBlock) {
            // Get driver details from the store or the message
            double distanceScore = calculateDistanceScore(driver, clientLatitude, clientLongitude);
            double genderScore = calculateGenderScore(driver, genderPreference);
            double ratingScore = calculateRatingScore(driver.getRating());
            double salaryScore = calculateSalaryScore(driver.getSalary());

            double matchScore = distanceScore * 0.4 + genderScore * 0.1 + ratingScore * 0.3 + salaryScore * 0.2;

            if (matchScore > highestMatchScore) {
                highestMatchScore = matchScore;
                bestMatch = driver;
            }
        }

        if (bestMatch != null) {
            // Construct the match JSON object to send to the match stream
            Map<String, Integer> match = new HashMap<>();
            match.put("clientId", clientId);
            match.put("driverId", bestMatch.getDriverId());
            String matchJson = toJson(match);
            
            // Send the match to the match-stream
            collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "match-stream"), matchJson));
        }
    }

    // Helper methods for calculating scores  <<<<<<<!!!!!!!!!!!!!!<<<<<<<!!!!!!!!!!!!!!
    private double calculateDistanceScore(Map<String, Object> driverData, double clientLatitude, double clientLongitude) {
        double driverLatitude = (Double) driverData.get("latitude");
        double driverLongitude = (Double) driverData.get("longitude");

        // Calculate the Euclidean distance
        double distance = Math.sqrt(Math.pow(driverLatitude - clientLatitude, 2) + Math.pow(driverLongitude - clientLongitude, 2));

        // Calculate the score using the distance
        double distanceScore = Math.exp(-1 * distance);

        return distanceScore;
    }

    //<<<<<<<!!!!!!!!!!!!!!<<<<<<<!!!!!!!!!!!!!!<<<<<<<!!!!!!!!!!!!!!
    private double calculateGenderScore(Driver driver, String genderPreference) {
       // Calculate the gender score based on driver gender and client preference
       String driverGender = driver.getGender(); // Assume Driver class has a getGender() method.

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
    // Example of calculating match score (you will need actual driver details and client preferences)
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
    private String toJson(Map<String, Integer> data) {
        try {
            return objectMapper.writeValueAsString(data);
        } catch (IOException e) {
            throw new RuntimeException("Error serializing data to JSON", e);
        }
    }

    //<<<<<<<!!!!!!!!!!!!!!<<<<<<<!!!!!!!!!!!!!!<<<<<<<!!!!!!!!!!!!!!
    // Assume a Driver class is defined elsewhere with appropriate fields and methods
    private List<Driver> getAllDriversInBlock(Integer blockId) {
        // Implement logic to retrieve all drivers in the block from the driverLocationStore
    }


    private void sendMatchToOutputStream(MessageCollector collector, Integer clientId, Integer driverId) {
        Map<String, Object> matchMap = new HashMap<>();
        matchMap.put("clientId", clientId);
        matchMap.put("driverId", driverId);

        String matchJson = jsonUtil.toJson(matchMap);
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "match-stream"), matchJson));
    }

}
