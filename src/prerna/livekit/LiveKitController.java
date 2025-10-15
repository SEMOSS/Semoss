package prerna.livekit;

import prerna.livekit.LiveKitController;
import prerna.util.Utility;
import io.livekit.server.AccessToken;
import io.livekit.server.RoomName;
import io.livekit.server.RoomJoin;
import io.livekit.server.RoomServiceClient;
import livekit.LivekitModels.Room;
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
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.py.PyTranslator;
import prerna.om.Insight;
import prerna.util.Settings;
import prerna.engine.api.IModelEngine;



public class LiveKitController {
	/*
	 * Record to track each operation type we support in Python and whether it requires the model to
	 * support real-time operations
	 */
	private record OperationInfo(String name, boolean requiresRealtime) {}

	private static final Map<String, OperationInfo> OPERATIONS = Map.of(
	    "turn_based_transcription", new OperationInfo("turn_based_transcription", false),
	    "realtime_transcription",   new OperationInfo("realtime_transcription", true)
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
	private record ModelDetails(String model, String modelType, String apiKey, boolean realtimeSupport, String modelUrl) {
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

	        String rt = p.getProperty("REALTIME", "false");
	        boolean realtime = "true".equalsIgnoreCase(rt) || "1".equals(rt);
	        // TODO: How am I grabbing custom model URLs for hosted models...
	        String modelUrl = "";

	        return new ModelDetails(model, modelType, apiKey, realtime, modelUrl);
	    }
	}
	
	
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
	
	public String mintPyListenerJwt(String roomId) {
		
		AccessToken token = new AccessToken(liveKitKey, liveKitSecret);
		
		token.setName("python-listener");
		token.setIdentity("python-listener");
		token.setRoomConfiguration(null);
		token.addGrants(new RoomJoin(true), new RoomName(roomId));
		
		return token.toJwt();
	}
	
	public AccessToken joinRoom(String userName, String userId, String roomId, String modelId, String aiOperation, Insight insight) throws Exception {
		Boolean roomExists = checkIfRoomExists(roomId);
		
		if (!roomExists) {
			createRoom(roomId);
		}
		
		ModelDetails modelDetails = getModelDetails(modelId);
		
		String pythonListener = createLiveKitToPipecatPipeline(roomId, aiOperation, modelDetails, insight);

		classLogger.info(pythonListener);
		
		return mintJwt(userName, userId, roomId);
	}
	
	public Room createRoom(String roomId) throws IOException {
		Call<Room> call = room_client.createRoom(roomId);
		Response<Room> response = call.execute();
		return response.body();
	}
	
	public List<Room> listRooms() throws IOException {
		Call<List<Room>> call = room_client.listRooms();
		Response<List<Room>> response = call.execute();
		List<Room> rooms = response.body();
		return rooms;
	}
	
	public Boolean checkIfRoomExists(String roomId) throws IOException {
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
	public boolean getServerHealth() {
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
		String pyJwt = mintPyListenerJwt(roomName);
	
		PyTranslator pyTranslator = insight.getPyTranslator();
		
		String importCommand = "from livekit_listener.lk_listener import join_as_listener";
		
		String icOutput = pyTranslator.runScript(importCommand) + "";
		
		String insightId = insight.getInsightId();
		
		String joinAsListenerCommand = "join_as_listener('%s', '%s', '%s', '%s')".formatted(roomName, pyJwt, liveKitUrl, insightId);
		
		String listenerJoinResult = pyTranslator.runScript(joinAsListenerCommand) + "";
		
		return listenerJoinResult;
	}
	
	protected String createLiveKitToPipecatPipeline(String roomName, String aiOperation, ModelDetails modelDetails, Insight insight) {
	    OperationInfo op = requireOperation(aiOperation);

	    if (op.requiresRealtime() && !modelDetails.realtimeSupport()) {
	        throw new IllegalArgumentException(
	            "Operation '" + op.name() + "' requires realtime support, " +
	            "but model '" + modelDetails.model() + "' does not have it."
	        );
	    }

		String pyJwt = mintPyListenerJwt(roomName);
		
		PyTranslator pyTranslator = insight.getPyTranslator();
		
		String importCommand = "from pcat.lk_to_pcat import join_as_listener";
		
		String icOutput = pyTranslator.runScript(importCommand) + "";
		
		String insightId = insight.getInsightId();
		
	    String joinAsListenerCommand = String.format(
	            "join_as_listener(room_name='%s', jwt='%s', url='%s', operation='%s', model='%s', model_type='%s', api_key='%s', model_url='%s', insight_id='%s')",
	            roomName,
	            pyJwt,
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
	
	protected ModelDetails getModelDetails(String engineId) throws Exception {
	    IModelEngine modelEngine = Utility.getModel(engineId);
	    Properties smssProp = modelEngine.getSmssProp();
	    return ModelDetails.from(smssProp);
	}
}
