/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.livekit;

import prerna.util.Utility;
import io.livekit.server.AccessToken;
import io.livekit.server.RoomName;
import io.livekit.server.RoomJoin;
import io.livekit.server.RoomServiceClient;
import livekit.LivekitModels.Room;
import livekit.LivekitModels.ParticipantInfo;
import retrofit2.Call;
import retrofit2.Response;

import java.util.Properties;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.util.Settings;
import prerna.engine.api.IModelEngine;



public class LiveKitController {
	// -----------------START RECORDS -----------------------------
	
	/*
	 * Record to track each operation type we support in Python and whether it requires the model to
	 * support real-time operations
	 */
	private record OperationInfo(String name, boolean requiresRealtime) {}

	private static final Map<String, OperationInfo> OPERATIONS = Map.of(
	    "turn_based_transcription", new OperationInfo("turn_based_transcription", false),
	    "turn_based_translation",   new OperationInfo("turn_based_translation", true),
	    "speech_to_speech_realtime", new OperationInfo("speech_to_speech_realtime", true)
	);

	private static OperationInfo requireOperation(String opName) {
	    if (opName == null) {
	        throw new IllegalArgumentException("aiOperation is null.");
	    }
	    var key = opName.trim().toLowerCase();
	    var op = OPERATIONS.get(key);
	    if (op == null) {
	        String allowed = String.join(", ", OPERATIONS.keySet());
	        throw new IllegalArgumentException("Unknown aiOperation: '" + opName +
	            "'. Allowed values: " + allowed);
	    }
	    return op;
	}
	
	/*
	 * Record for the required model details that I need to send to the Python server
	 */
	private record ModelDetails(String model, String modelType, String apiKey, boolean realtimeSupport, String modelUrl, String awsAccessKey, String awsSecretKey) {
	    ModelDetails {
	        if (model == null || model.isBlank()) {
	            throw new IllegalArgumentException("Model is not defined in SMSS file.");
	        }
	        if (modelType == null || modelType.isBlank()) {
	            throw new IllegalArgumentException("Model Type is not defined in SMSS file.");
	        }
	        apiKey = apiKey == null ? "" : apiKey.trim();
	    }

	    static ModelDetails from(Properties p) {
	        String model = p.getProperty(Settings.MODEL, "").trim();
	        String modelType = p.getProperty(Settings.MODEL_TYPE, "").trim();
	        String apiKey = p.getProperty("OPEN_AI_KEY", "").trim();
	        String awsAccessKey = p.getProperty("AWS_ACCESS_KEY", "").trim();
	        String awsSecretKey = p.getProperty("AWS_SECRET_KEY", "").trim();
	        String rt = p.getProperty("REALTIME", "false");
	        boolean realtime = "true".equalsIgnoreCase(rt) || "1".equals(rt);
	        // TODO: How am I grabbing custom model URLs for hosted models...
	        String modelUrl = "";

	        return new ModelDetails(model, modelType, apiKey, realtime, modelUrl, awsAccessKey, awsSecretKey);
	    }
	}
	
	private record PyToken(String token, String identity) {}
	
	// -----------------END RECORDS -----------------------------
	
	
	private static final Logger classLogger = LogManager.getLogger(LiveKitController.class);

	String liveKitKey;
	String liveKitSecret;
	String liveKitUrl;
	
	private static final HttpClient httpClient = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(5))
			.build();
	
	RoomServiceClient room_client = null;
	
	public LiveKitController() {
		liveKitKey = System.getenv("LIVEKIT_KEY");
		if (liveKitKey == null) {
			liveKitKey = Utility.getDIHelperProperty("LIVEKIT_KEY");
		}
		liveKitSecret = System.getenv("LIVEKIT_SECRET");
		if (liveKitSecret == null) {
			liveKitSecret = Utility.getDIHelperProperty("LIVEKIT_SECRET");
		}
		liveKitUrl = System.getenv("LIVEKIT_URL");
		if (liveKitUrl == null) {
			liveKitUrl = Utility.getDIHelperProperty("LIVEKIT_URL");
		}
	
		if (liveKitKey == null || liveKitSecret == null || liveKitUrl == null) {
			throw new NullPointerException("Must define LiveKit key, secret and url.");
		}
		
		Boolean isServerHealthy = getServerHealth();
		if (!isServerHealthy) {
			throw new RuntimeException("The LiveKit server is not healthy.");
		}
		
		room_client = RoomServiceClient.createClient(liveKitUrl,liveKitKey,liveKitSecret);
	}
	
	
	public AccessToken mintJwt(String userName, String userId, String roomId) {
		Boolean isServerHealthy = getServerHealth();
		if (!isServerHealthy) {
			throw new RuntimeException("The LiveKit server is not healthy.");
		}
				
		AccessToken token = new AccessToken(liveKitKey, liveKitSecret);
		
		token.setName(userName);
		token.setIdentity(userId);
		token.setRoomConfiguration(null);
		token.addGrants(new RoomJoin(true), new RoomName(roomId));
		
		return token;
	}
	

	public PyToken mintPyListenerJwt(String roomId) {
		AccessToken token = new AccessToken(liveKitKey, liveKitSecret);
		
		token.setName("python-listener");
		token.setIdentity("python-listener");
		token.setRoomConfiguration(null);
		token.addGrants(new RoomJoin(true), new RoomName(roomId));
		
		return new PyToken(token.toJwt(), token.getIdentity());
	}
	
	
	public AccessToken joinRoom(String userName, String userId, String roomId, String modelId, String aiOperation, Insight insight, Map<String, Object> paramMap) throws Exception {
		Boolean roomExists = checkIfRoomExists(roomId);
		
		if (!roomExists) {
			createRoom(roomId);
		}
		
		ModelDetails modelDetails = getModelDetails(modelId);
		
		PyToken pyToken = mintPyListenerJwt(roomId);
		
		if (aiOperation.equalsIgnoreCase("turn_based_transcription") || aiOperation.equalsIgnoreCase("turn_based_translation") || aiOperation.equalsIgnoreCase("speech_to_speech_realtime")) {
			createLiveKitToPipecatPipeline(roomId, pyToken.token(), aiOperation, modelDetails, insight, paramMap);
		} else if(aiOperation.equalsIgnoreCase("real_time_transcription")) {
			createLiveKitToOpenAIRealTimePipeline(roomId, pyToken.token(), aiOperation, modelDetails, insight);
		}
		
		Boolean waitForPyParticipant = waitUntilParticipantPresent(roomId, pyToken.identity(), 40000);
		
		if (!waitForPyParticipant) {
			throw new RuntimeException("Py participant failed to join the room"); 
		} else {
			classLogger.info("Py listener joined the room");
		}
		
		return mintJwt(userName, userId, roomId);
	}
	
	
	private Room createRoom(String roomId) throws IOException {
		Call<Room> call = room_client.createRoom(roomId);
		Response<Room> response = call.execute();
		return response.body();
	}
	
	
	public List<Room> listRooms() throws IOException {
		Call<List<Room>> call = room_client.listRooms();
		Response<List<Room>> response = call.execute();
		return response.body();
	}
	
	
	private List<ParticipantInfo> listParticipants(String roomId) throws IOException {
		Call<List<ParticipantInfo>> call = room_client.listParticipants(roomId);
		Response<List<ParticipantInfo>> response = call.execute();
	    if (!response.isSuccessful() || response.body() == null) {
	        return Collections.emptyList();
	    }
		return response.body();
	}
	
	private boolean waitUntilParticipantPresent(String roomId, String identity, long timeoutMillis) {
	    long startTime = System.currentTimeMillis();

	    while (System.currentTimeMillis() - startTime < timeoutMillis) {
	        try {
	            List<ParticipantInfo> participants = listParticipants(roomId);

	            for (ParticipantInfo p : participants) {
	                if (p.getIdentity().equalsIgnoreCase(identity)) {
	                    return true;
	                }
	            }

	        } catch (Exception e) {
	            String errorMsg = "Failed to find participant: " + e.getMessage();
	            classLogger.error(errorMsg, e);
	        }

	        try {
	            Thread.sleep(1000);
	        } catch (InterruptedException ie) {
	            Thread.currentThread().interrupt();
	            return false;
	        }
	    }

	    classLogger.warn("Timed out waiting for participant '{}' in room '{}'", identity, roomId);
	    return false;
	}

	
	private Boolean checkIfRoomExists(String roomId) throws IOException {
		List<Room> rooms = listRooms();
		
		for (Room room: rooms) {
			if (room.getName().equals(roomId)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Performs a health check on the LiveKit URL to ensure it's accessible
	 * @return true if the server responds with OK status, false otherwise
	 */
	private boolean getServerHealth() {
		try {
			String healthCheckUrl = liveKitUrl;
			if (!healthCheckUrl.endsWith("/")) {
				healthCheckUrl += "/";
			}
			
			classLogger.info("Checking LiveKit health at URL: {}", healthCheckUrl);
			
			URI uri = new URI(healthCheckUrl);
			
			HttpRequest request = HttpRequest.newBuilder()
					.uri(uri)
					.timeout(Duration.ofSeconds(5))
					.GET()
					.build();
			
			HttpResponse<String> response = httpClient.send(request, 
					HttpResponse.BodyHandlers.ofString());
			
			int statusCode = response.statusCode();
			
			Boolean healthyStatus = statusCode >= 200 && statusCode < 300;
			
			if (healthyStatus) {
				classLogger.info("LiveKit service is healthy");
				return true;
			} else {
				classLogger.info("LiveKit service failed the health check with status code: {}", statusCode);
				return false;
			}
			
			   
		} catch (URISyntaxException e) {
			classLogger.error("Invalid LiveKit URL format: " + liveKitUrl + 
							   ". Error: " + e.getMessage());
			return false;
		} catch (IOException | InterruptedException e) {
			classLogger.error("Health check failed for LiveKit URL: " + liveKitUrl + 
							   ". Error: " + e.getMessage());
			return false;
		}
	}
	
	// This is not being used. It is for the direct LiveKit listener class if I chose to keep it..
	protected String createPythonListener(String roomName, Insight insight) {
		PyToken pyJwt = mintPyListenerJwt(roomName);
	
		PyTranslator pyTranslator = insight.getPyTranslator();
		
		String importCommand = "from audio.lk_listener import join_as_listener";
		
		String icOutput = pyTranslator.runScript(importCommand) + "";
		
		String insightId = insight.getInsightId();
		
		String joinAsListenerCommand = "join_as_listener('%s', '%s', '%s', '%s')".formatted(roomName, pyJwt.token(), liveKitUrl, insightId);
		
		String listenerJoinResult = pyTranslator.runScript(joinAsListenerCommand) + "";
		
		return listenerJoinResult;
	}
	
	protected String createLiveKitToOpenAIRealTimePipeline(String roomName, String token, String aiOperation, ModelDetails modelDetails, Insight insight) {
	    OperationInfo op = requireOperation(aiOperation);

	    if (op.requiresRealtime() && !modelDetails.realtimeSupport()) {
	        throw new IllegalArgumentException(
	            "Operation '" + op.name() + "' requires realtime support, " +
	            "but model '" + modelDetails.model() + "' does not support it."
	        );
	    }
		
		PyTranslator pyTranslator = insight.getPyTranslator();
		
		String importCommand = "from audio.lk_listener import join_as_listener";
		pyTranslator.runScript(importCommand);
		
		String insightId = insight.getInsightId();
		
	    String joinAsListenerCommand = String.format(
	            "join_as_listener(room_name='%s', jwt='%s', url='%s', operation='%s', model='%s', model_type='%s', api_key='%s', model_url='%s', insight_id='%s')",
	            roomName,
	            token,
	            liveKitUrl,
	            op.name(),
	            modelDetails.model(),
	            modelDetails.modelType(),
	            modelDetails.apiKey(),
	            modelDetails.modelUrl(),
		        insightId
	        );

		
		return pyTranslator.runScript(joinAsListenerCommand) + "";		
	}
	
	protected String createLiveKitToPipecatPipeline(String roomName, String token, String aiOperation, ModelDetails modelDetails, Insight insight, Map<String, Object> paramMap) {
	    OperationInfo op = requireOperation(aiOperation);

	    if (op.requiresRealtime() && !modelDetails.realtimeSupport()) {
	        throw new IllegalArgumentException(
	            "Operation '" + op.name() + "' requires realtime support, " +
	            "but model '" + modelDetails.model() + "' does not support it."
	        );
	    }
		
	    PyTranslator pyTranslator = insight.getPyTranslator();
	    
	    String pythonParamMap = prerna.ds.py.PyUtils.determineStringType(paramMap);
	    
	    String importCommand = "from audio.lk_to_pcat import join_as_listener";
	    pyTranslator.runScript(importCommand);
	    
	    String insightId = insight.getInsightId();
	    
	    String joinAsListenerCommand = String.format(
	            "join_as_listener(room_name='%s', jwt='%s', url='%s', operation='%s', model='%s', model_type='%s', api_key='%s', model_url='%s', insight_id='%s', aws_access_key='%s', aws_secret_key='%s', param_map=%s)",
	            roomName,
	            token,
	            liveKitUrl,
	            op.name(),
	            modelDetails.model(),
	            modelDetails.modelType(),
	            modelDetails.apiKey(),
	            modelDetails.modelUrl(),
	            insightId,
	            modelDetails.awsAccessKey(),
	            modelDetails.awsSecretKey(),
	            pythonParamMap 
	    );

	    return pyTranslator.runScript(joinAsListenerCommand) + "";       
	}
	
	protected ModelDetails getModelDetails(String engineId) throws Exception {
	    IModelEngine modelEngine = Utility.getModel(engineId);
	    Properties smssProp = modelEngine.getSmssProp();
	    return ModelDetails.from(smssProp);
	}
}
