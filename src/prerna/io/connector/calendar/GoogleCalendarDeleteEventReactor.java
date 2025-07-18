package prerna.io.connector.calendar;

import java.io.IOException;
import java.util.*;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.Event;
import com.google.api.services.calendar.model.Events;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarDeleteEventReactor extends AbstractReactor {
	
	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String AppName = "Google Docs";
	
	public GoogleCalendarDeleteEventReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.SUMMARY.getKey()};
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String summary = this.keyValue.get(this.keysToGet[0]);
		try {
			String accessToken = getGoogleAccessToken();
			Calendar CalendarService = getCalendarServiceUsingToken(accessToken);
			String id = getEventID(CalendarService, summary);
			boolean result = GoogleCalendarHelper.deleteEvent(CalendarService, id);
			return new NounMetadata(result, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
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
	
	public static String getEventID(Calendar service, String summary) throws Exception {
	    String pageToken = null;

	    while (true) {
	        Events events = service.events().list("primary")
	            .setOrderBy("startTime")
	            .setSingleEvents(true)
	            .setMaxResults(100)
	            .setPageToken(pageToken)
	            .execute();

	        List<Event> items = events.getItems();

	        for (Event item : items) {
	            if (item.getSummary() != null && item.getSummary().trim().equalsIgnoreCase(summary.trim())) {
	            	if(item.getRecurringEventId() == null) {
	            		return item.getId();
	            	}
	            	else {
	            		return item.getRecurringEventId();
	            	}
	            }
	        }

	        pageToken = events.getNextPageToken();
	        if (pageToken == null) {
	            break;
	        }
	    }

	    return null;
	}

}
