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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ICodeExecution;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.om.Insight;

public class LiveKitController {
	private static final Logger classLogger = LogManager.getLogger(LiveKitController.class);

	private static LiveKitController instance;
	String liveKitKey;
	String liveKitSecret;
	String liveKitUrl;
	
	private static final HttpClient httpClient = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(5))
			.build();
	
	RoomServiceClient room_client = null;
	
	private LiveKitController() {
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
	
    public static LiveKitController getInstance() {
        if (instance == null) {
            instance = new LiveKitController();
        }
        return instance;
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
	
	public AccessToken joinRoom(String userName, String userId, String roomId, Insight insight) throws IOException {
		Boolean roomExists = checkIfRoomExists(roomId);
		
		if (!roomExists) {
			createRoom(roomId);
		}
		
		String pythonListener = createPythonListener(roomId, insight);
		
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
	
}
