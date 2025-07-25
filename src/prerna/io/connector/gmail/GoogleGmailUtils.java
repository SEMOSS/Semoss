package prerna.io.connector.gmail;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.gmail.Gmail;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.execptions.SemossPixelException;

public class GoogleGmailUtils {

	private static final Logger classLogger = LogManager.getLogger(GoogleGmailUtils.class);
	
	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String AppName = "Google Docs";

	public static Gmail getGmailServiceUsingToken(String token) throws Exception {
		HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {

			@Override
			public void initialize(HttpRequest request) throws IOException {
				request.getHeaders().setAuthorization("Bearer " + token);

			}
		};
		return new Gmail.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY, requestInitializer)
				.setApplicationName(AppName).build();
	}
	
	public static String getGoogleAccessToken(User user) throws Exception {
        String accessToken = null;

        if (user == null) {
        	classLogger.error("User not found in session.");
            throw new SemossPixelException("User not found in session.");
        }

        AccessToken googleToken = user.getAccessToken(AuthProvider.GOOGLE);

        if (googleToken == null) {
        	classLogger.error("No Google Access Token fetched for user");
            throw new SemossPixelException("No Google Access Token fetched.");
        }

        accessToken = googleToken.getAccess_token();
        return accessToken;
    }
}
