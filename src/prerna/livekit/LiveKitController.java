package prerna.livekit;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
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
import java.util.Arrays;
import java.util.UUID;
import prerna.auth.User;
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
		
		Boolean isServerHealthy = isLiveKitUrlHealthy();
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
		Boolean isServerHealthy = isLiveKitUrlHealthy();
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
	
	/**
	 * Performs a health check on the LiveKit URL to ensure it's accessible
	 * @return true if the server responds with OK status, false otherwise
	 */
	public boolean isLiveKitUrlHealthy() {
		try {
			String healthCheckUrl = liveKitUrl;
//			if (!healthCheckUrl.endsWith("/")) {
//				healthCheckUrl += "/";
//			}
			
			URI uri = new URI(healthCheckUrl);
			
			HttpRequest request = HttpRequest.newBuilder()
					.uri(uri)
					.timeout(Duration.ofSeconds(5))
					.GET()
					.build();
			
			HttpResponse<String> response = httpClient.send(request, 
					HttpResponse.BodyHandlers.ofString());
			
			int statusCode = response.statusCode();
			
			return statusCode >= 200 && statusCode < 300;
			   
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
