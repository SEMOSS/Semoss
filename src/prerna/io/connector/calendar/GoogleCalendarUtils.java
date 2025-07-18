package prerna.io.connector.calendar;

import java.io.IOException;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.calendar.Calendar;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;

public class GoogleCalendarUtils {

	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String AppName = "Google Docs";

	public static Calendar getCalendarServiceUsingToken(String token) throws Exception {
		HttpRequestInitializer requestInitializer = new HttpRequestInitializer() {

			@Override
			public void initialize(HttpRequest request) throws IOException {
				request.getHeaders().setAuthorization("Bearer " + token);

			}
		};
		return new Calendar.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY, requestInitializer)
				.setApplicationName(AppName).build();
	}

	public static String getGoogleAccessToken(User user) throws Exception {
		String accessToken = null;

		if (user == null) {
			throw new Exception("User not found in session.");
		}

		AccessToken googleToken = user.getAccessToken(AuthProvider.GOOGLE);

		if (googleToken == null) {
			throw new Exception("No Google Access Token fetched.");
		}
		accessToken = googleToken.getAccess_token();
		return accessToken;
	}
}
