package prerna.io.connector.calendar;

import java.io.IOException;
import java.util.Map;
import java.util.*;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarCreateEventReactor extends AbstractReactor {

	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String AppName = "Google Docs";

	public GoogleCalendarCreateEventReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SUMMARY.getKey(), ReactorKeysEnum.LOCATION.getKey(),
				ReactorKeysEnum.DESCRIPTION.getKey(), ReactorKeysEnum.STARTDATE.getKey(),
				ReactorKeysEnum.ENDDATE.getKey(), ReactorKeysEnum.EMAIL.getKey(), ReactorKeysEnum.VIDEO.getKey() };
		this.keyRequired = new int[] { 1, 1, 1, 1, 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {

		this.organizeKeys();
		String summary = this.keyValue.get(this.keysToGet[0]);
		String location = this.keyValue.get(this.keysToGet[1]);
		String desc = this.keyValue.get(this.keysToGet[2]);
		String startdatetime = this.keyValue.get(this.keysToGet[3]);
		String enddatetime = this.keyValue.get(this.keysToGet[4]);
		String emailsInput = this.keyValue.get(this.keysToGet[5]);
		String enablevideo = this.keyValue.get(this.keysToGet[6]);

		try {
			String accessToken = getGoogleAccessToken();
			Calendar CalendarService = getCalendarServiceUsingToken(accessToken);
			
			List<String> attendeeEmails = new ArrayList<>();
			if (emailsInput != null && !emailsInput.isEmpty()) {
			    String[] emailArray = emailsInput.split(",");
			    for (String email : emailArray) {
			        email = email.trim();
			        if (!email.isEmpty()) {
			            attendeeEmails.add(email);
			        }
			    }
			}
			boolean video = Boolean.parseBoolean(enablevideo);
			Event createdEvent = GoogleCalendarHelper.createEvent(CalendarService, summary, location, desc, startdatetime,
					enddatetime, attendeeEmails, video);
			Map<String, Object> mp = new HashMap<>();
			mp.put("id", createdEvent.getId());
			mp.put("Link", createdEvent.getHtmlLink());
			return new NounMetadata(mp, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Issue with input");
		}

	}

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

	public String getGoogleAccessToken() throws Exception {

		String accessToken = null;
		User user = this.insight.getUser();

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
