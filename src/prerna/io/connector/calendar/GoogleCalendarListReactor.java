package prerna.io.connector.calendar;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.util.DateTime;
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

public class GoogleCalendarListReactor extends AbstractReactor{
	
	private static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();
	private static final String AppName = "Google Docs";
	
	public GoogleCalendarListReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.STARTDATE.getKey(), ReactorKeysEnum.ENDDATE.getKey()};
		this.keyRequired = new int[] { 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String startdate = this.keyValue.get(this.keysToGet[0]);
		String enddate = this.keyValue.get(this.keysToGet[1]);
		try {
			String accessToken = getGoogleAccessToken();
			Calendar CalendarService = getCalendarServiceUsingToken(accessToken);

			List<String> eventList = getEventList(CalendarService, startdate, enddate);
			return new NounMetadata(eventList, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			throw new SemossPixelException("Issue with input");
		}
	}
	
	public static List<String> getEventList(Calendar service, String startDateTime, String endDateTime)
			throws Exception {
		List<String> eventList = new ArrayList<>();
		DateTime timeMin = new DateTime(startDateTime);
		DateTime timeMax = new DateTime(endDateTime);

		Events events = service.events().list("primary").setOrderBy("startTime").setSingleEvents(true)
				.setTimeMin(timeMin).setTimeMax(timeMax).setMaxResults(100).execute();

		List<Event> items = events.getItems();

		if (items.isEmpty()) {
			System.out.println("No Events Found In The Given Date Range");
		} else {
			for (Event item : items) {
				eventList.add(item.getSummary());
			}
		}

		return eventList;
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
