package com.microsoft.azure.kusto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.transit.realtime.GtfsRealtime.*;
import com.google.transit.realtime.GtfsRealtime.TripUpdate.StopTimeUpdate;
import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.EventHubOutput;
import com.microsoft.azure.functions.annotation.FunctionName;
import com.microsoft.azure.functions.annotation.TimerTrigger;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Azure Functions with Timer trigger.
 */
public class TimeBasedPoller {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final String vehiclePositionsEndpoint = System.getenv("VEHICLE_POSITIONS_ENDPOINT");

    private static String getFeeds(@NotNull String endPoint) throws IOException {
        URL url = new URL(endPoint);
        URLConnection httpURLConnection = url.openConnection();
        httpURLConnection.setRequestProperty("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36");
        httpURLConnection.connect();
        try (InputStream in = httpURLConnection.getInputStream()) {
            FeedMessage feed = FeedMessage.parseFrom(in);
            for (FeedEntity entity : feed.getEntityList()) {
                if (entity.hasTripUpdate()) {
                    return extractTripUpdates(entity);
                }
                if (entity.hasVehicle()) {
                    return getVehicleDetails(entity);
                }
            }
        }
        throw new IllegalStateException("Feed entities supported for Trips and Vehicle Positions");
    }

    private static @NotNull String getVehicleDetails(@NotNull FeedEntity entity) throws JsonProcessingException {
        VehiclePosition vehiclePosition = entity.getVehicle();
        ObjectNode vehicleFields = mapper.createObjectNode();
        vehicleFields.put("tripId", vehiclePosition.getTrip().getTripId());
        vehicleFields.put("routeId", vehiclePosition.getTrip().getRouteId());
        vehicleFields.put("directionId", vehiclePosition.getTrip().getDirectionId());
        vehicleFields.put("currentStopSequence", vehiclePosition.getCurrentStopSequence());
        vehicleFields.put("currentStatus", vehiclePosition.getCurrentStatus().getValueDescriptor().toString());
        vehicleFields.put("timestamp", vehiclePosition.getTimestamp());
        vehicleFields.put("stopId", vehiclePosition.getStopId());
        vehicleFields.put("vehicleId", vehiclePosition.getVehicle().getId());
        vehicleFields.put("vehicleLabel", vehiclePosition.getVehicle().getLabel());
        vehicleFields.put("vehicleLicensePlate", vehiclePosition.getVehicle().getLicensePlate());
        vehicleFields.put("occupancyStatus", vehiclePosition.getOccupancyStatus().getValueDescriptor().toString());
        Position currentPosition = vehiclePosition.getPosition();
        vehicleFields.put("lat", currentPosition.getLatitude());
        vehicleFields.put("lon", currentPosition.getLongitude());
        vehicleFields.put("bearing", currentPosition.getBearing());
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(vehicleFields);
    }

    private static String extractTripUpdates(@NotNull FeedEntity entity) throws IOException {
        TripUpdate tripUpdate = entity.getTripUpdate();
        int noStopTimeUpdates = tripUpdate.getStopTimeUpdateCount();
        int tripDelay = tripUpdate.getDelay();
        long tripTimestamp = tripUpdate.getTimestamp();
        TripDescriptor tripDescriptor = tripUpdate.getTrip();
        ObjectNode headerFields = mapper.createObjectNode();
        headerFields.put("tripId", tripDescriptor.getTripId());
        headerFields.put("routeId", tripDescriptor.getRouteId());
        headerFields.put("directionId", tripDescriptor.getDirectionId());
        headerFields.put("schedule", tripDescriptor.getScheduleRelationship().getValueDescriptor().toString());
        headerFields.put("date", tripDescriptor.getStartDate());
        headerFields.put("time", tripDescriptor.getStartTime());
        headerFields.put("noStopUpdates", noStopTimeUpdates);
        headerFields.put("tripDelay", tripDelay);
        headerFields.put("tripTimestamp", tripTimestamp);
        ArrayNode stopTimeUpdates = mapper.createArrayNode();
        for (StopTimeUpdate stoptimeUpdate : tripUpdate.getStopTimeUpdateList()) {
            ObjectNode stopTimeUpdate = mapper.createObjectNode();
            stopTimeUpdate.put("stopId", stoptimeUpdate.getStopId());
            stopTimeUpdate.put("stopSequence", stoptimeUpdate.getStopSequence());
            stopTimeUpdate.put("schedule", stoptimeUpdate.getScheduleRelationship().getValueDescriptor().toString());
            stopTimeUpdate.put("arrivalTime", stoptimeUpdate.getArrival().getTime() * 1000);
            stopTimeUpdate.put("arrivalDelay", stoptimeUpdate.getArrival().getDelay());
            stopTimeUpdate.put("departureTime", stoptimeUpdate.getDeparture().getTime() * 1000);
            stopTimeUpdate.put("departureDelay", stoptimeUpdate.getDeparture().getDelay());
            stopTimeUpdates.add(stopTimeUpdate);
        }
        headerFields.putIfAbsent("stopTimeUpdates", stopTimeUpdates);
        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(headerFields);
    }

    /**
     * This function will be invoked periodically according to the specified
     * schedule.
     */
    @FunctionName("TimeBasedPoller")
    @EventHubOutput(name = "vehicle-events", eventHubName = "%EVENT_HUB_NAME%",
            connection = "GTFSRealtimeEventHubConnection")
    public String run(
            @TimerTrigger(name = "timerInfo", schedule = "%SCHEDULE_CRON%") String timerInfo,
            final ExecutionContext context

    ) throws Exception {
        try {
            context.getLogger().fine("Getting vehicle positions at : " +
                    LocalDateTime.now(Clock.system(ZoneId.of("Asia/Kolkata"))));
            return getFeeds(vehiclePositionsEndpoint);
        } catch (Exception e) {
            context.getLogger().severe(e.getMessage());
            throw e;
        }
    }
}


// String endPoint = "http://api.nextlift.ca/gtfs-realtime/tripupdates.pb";
// String endPoint = "https://www.rtd-denver.com/files/gtfs-rt/TripUpdate.pb";
// https://www.mbta.com/developers/v3-api/streaming
// String endPoint = "http://api.bart.gov/gtfsrt/tripupdate.aspx";
