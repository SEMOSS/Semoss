package prerna.engine.impl.model;

import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tinkerpop.shaded.jackson.databind.JsonMappingException;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.sql.Blob;
import java.sql.SQLException;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.model.inferencetracking.reactors.UpdateRoomOptionsReactor;
import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.util.Utility;

/**
 * Utility methods for fetching and managing Room objects.
 * - createRoomIfNotExists: creates (if needed) and returns a Room
 * - getOrLoadRoom: looks up or loads room to memory hash, but never creates a Room
 */
public class RoomUtils {

    private static final Logger logger = LogManager.getLogger(RoomUtils.class);

    /**
     * Ensures a Room exists: creates it if necessary, then loads it for the given user/insight.
     * @return the existing or newly created Room
     */
    public static Room createRoomIfNotExists(
            String roomId,
            Insight insight,
            IModelEngine modelEngine,
            String question
    ) {
        // Use the passed roomId or fallback to the insightId if null/empty
        if (roomId == null || roomId.trim().isEmpty()) {
            roomId = insight.getInsightId();
        }

        boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckConversationExists(roomId);

        if (!roomExistsInDB) {
            String agentType = null;
            String engineId = null;
            if (modelEngine != null) {
                agentType = modelEngine.getCatalogSubType(modelEngine.getSmssProp());
                engineId = modelEngine.getEngineId();
            }
            User user = insight.getUser();
            AccessToken userToken = user.getPrimaryLoginToken();
            String userName = userToken.getName();
            String userEmail = userToken.getEmail();
            String projectId = insight.getContextProjectId();
            if (projectId == null) {
                projectId = insight.getProjectId();
            }
            String projectName = null;
            if (projectId != null) {
                IProject project = Utility.getProject(projectId);
                projectName = project != null ? project.getProjectName() : null;
            }
            String roomName = (question != null) ? question.substring(0, Math.min(question.length(), 100)) : "untitled";
            ModelInferenceLogsUtils.doCreateNewConversation(
                    roomId,
                    roomName,
                    null,
                    userToken.getId(),
                    userName,
                    userEmail,
                    agentType,
                    engineId,
                    true,
                    projectId,
                    projectName
            );
            // Always get the loaded room object (avoiding any skipping, ensures in-memory cache is filled)
            return RoomUtils.getOrLoadRoom(roomId, insight);
        } else {
            return RoomUtils.getOrLoadRoom(roomId, insight);
        }
    }

    /**
     * Loads a Room from user room hash or database if present.
     * @throws IllegalArgumentException if Room does not exist.
     */
    public static Room getOrLoadRoom(String roomId,  Insight insight) {
        Room room;
        // Check in user's cache (roomHash)
        if (insight.getUser().roomHash.containsKey(roomId)) {
            try {
                room = (Room) insight.getUser().roomHash.get(roomId);
                return room;
            } catch (ClassCastException e) {
                insight.getUser().roomHash.remove(roomId); // Clear corrupted cache entry
            }
        }
        boolean roomExistsInDB = ModelInferenceLogsUtils.doCheckConversationExists(roomId);
        if (!roomExistsInDB)
            throw new IllegalArgumentException("User room is not valid");
        room = ModelInferenceLogsUtils.getRoomById(roomId, insight.getUser().getPrimaryLoginToken().getId());
        room.setInsight(insight);
        room.parseMessages();
        insight.getUser().roomHash.put(roomId, room);
        return room;
    }

    /**
     * Gets the room options map 
     */
    public static Map<String, Object> getRoomOptions(String roomId, String userId) {

        Blob optionsBlob = ModelInferenceLogsUtils.getRoomOptions(roomId, userId);
        byte[] bdata = null;
		try {
			bdata = optionsBlob.getBytes(1, (int)optionsBlob.length());
		} catch (SQLException e) {
			e.printStackTrace();
		}
       
        String roomOptionsString = new String(bdata);

        Gson gson = new Gson();
        Type type = new TypeToken<Map<String, Object>>(){}.getType();
        Map<String, Object> map = null;
        try {
        map = gson.fromJson(roomOptionsString, type);
        } catch (Exception e) {
        	e.printStackTrace();
        }

        String logMessage = String.format("Found %s in room options", map.keySet());
        logger.info(logMessage);

        return map;
    }
}