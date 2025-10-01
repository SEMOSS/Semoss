package prerna.livekit;

import prerna.livekit.LiveKitController;
import prerna.util.Utility;
import io.livekit.server.AccessToken;
import io.livekit.server.RoomName;
import io.livekit.server.RoomJoin;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LiveKitController {
	private static final Logger classLogger = LogManager.getLogger(LiveKitController.class);

	private static LiveKitController instance;
	String liveKitKey;
	String liveKitSecret;
	String liveKitUrl;
	
	private static final HttpClient httpClient = HttpClient.newBuilder()
			.connectTimeout(Duration.ofSeconds(5))
			.build();
	
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
		
		classLogger.debug("LiveKit Key: {} LiveKit Secret: {}", liveKitKey, liveKitSecret);
		
		AccessToken token = new AccessToken(liveKitKey, liveKitSecret);
		
		token.setName(userName);
		token.setIdentity(userId);
		token.setRoomConfiguration(null);
		token.addGrants(new RoomJoin(true), new RoomName(roomId));
		
		return token;
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
	
}
