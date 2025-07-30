package prerna.io.connector.calendar;

import java.net.URLEncoder;
import java.util.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import prerna.security.HttpHelperUtility;
import prerna.auth.User;
import com.google.gson.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GoogleCalendarListReactor extends AbstractReactor{

	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarListReactor.class);
	
	private static final String EVENTS = "events";
	private static final String DATE = "date";
	private static final String CALENDAR_ID = "primary";
	private static final String RECURRING_EVENT_ID = "recurringEventId";
    private static final String BASE_URL = "https://www.googleapis.com/calendar/v3/calendars/%s/events";
    private static final String ORDER_BY = "orderBy";
    private static final String ORDER_BY_START_TIME = "startTime";
    private static final String SINGLE_EVENTS = "singleEvents";
    private static final String SINGLE_EVENTS_TRUE = "true";
    private static final String TIME_MIN = "timeMin";
    private static final String TIME_MAX = "timeMax";
    private static final String MAX_RESULTS = "maxResults";
    private static final String MAX_RESULTS_100 = "100";
    private static final String PAGE_TOKEN = "pageToken";
    private static final String ITEMS = "items";
    private static final String SUMMARY = "summary";
    private static final String ID = "id";
    private static final String START = "start";
    private static final String DATE_TIME = "dateTime";
    private static final String NEXT_PAGE_TOKEN = "nextPageToken";
	
	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
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
			User user = this.insight.getUser();
			String accessToken = GoogleCalendarUtils.getGoogleAccessToken(user);
			List<Map<String, Object>> eventList = getEventList(accessToken, startdate, enddate);
			return new NounMetadata(eventList, PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.error("Unauthorized access or Please provide valid input");
			throw new SemossPixelException("Please provide valid input: " + e.getMessage(), e);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static List<Map<String, Object>> getEventList(String accessToken, String startDateTime, String endDateTime) throws Exception {
        Map<String,List<List<String>>> events = new LinkedHashMap<>();
		String url = String.format(BASE_URL, CALENDAR_ID);
        String pageToken = null;
        do {
            Map<String, String> headers = GoogleCalendarHelper.getBearerHeader(accessToken);
            Map<String, String> params = new HashMap<>();
            params.put(ORDER_BY, ORDER_BY_START_TIME);
            params.put(SINGLE_EVENTS, SINGLE_EVENTS_TRUE);
            params.put(TIME_MIN, startDateTime);
            params.put(TIME_MAX, endDateTime);
            params.put(MAX_RESULTS, MAX_RESULTS_100);
            if (pageToken != null) {
                params.put(PAGE_TOKEN, pageToken);
            }
            StringBuilder fullUrl = new StringBuilder(url);
            fullUrl.append("?");
            for (Map.Entry<String, String> entry : params.entrySet()) {
                fullUrl.append(URLEncoder.encode(entry.getKey(), "UTF-8")).append("=").append(URLEncoder.encode(entry.getValue(), "UTF-8")).append("&");
            }
            fullUrl.setLength(fullUrl.length() - 1);
            String response = HttpHelperUtility.getRequest(fullUrl.toString(), headers, null, null, null);
            Map<String, Object> json = gson.fromJson(response, new TypeToken<Map<String, Object>>() {}.getType());
            List<Map<String, Object>> items = (List<Map<String, Object>>) json.get(ITEMS);
            if (items != null && !items.isEmpty()) {
                for (Map<String, Object> item : items) {
                    List<String> lst = new ArrayList<>();
                    lst.add((String) item.get(SUMMARY));
                    lst.add((String) item.get(ID));
                    String recurringEventId = (String) item.get(RECURRING_EVENT_ID);
                    if (recurringEventId != null) {
                        lst.add(recurringEventId);               
                    }
                    Map<String, Object> start = (Map<String, Object>) item.get(START);
                    String dateTime = (String) start.get(DATE_TIME);
                    String date = dateTime.substring(0,10);
                    if(!events.containsKey(date)) {
                    	events.put(date, new ArrayList<>());	
                    }
                    events.get(date).add(lst);
                }
            }
            pageToken = (String) json.get(NEXT_PAGE_TOKEN);
        } while (pageToken != null);
	    if (events.isEmpty()) {
	        classLogger.info("No Events Found In The Given Date Range");
	    }
	    List<Map<String, Object>> eventList = new ArrayList<>();
	    for (Map.Entry<String, List<List<String>>> entry : events.entrySet()) {
	        Map<String, Object> map = new LinkedHashMap<>();
	        map.put(DATE, entry.getKey());
	        map.put(EVENTS, entry.getValue());
	        eventList.add(map);
	    }
	    return eventList;
	}
	
	@Override
	public String getReactorDescription() {
		return "This reactor is used to get the list of events.";
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
	    if (key.equals(ReactorKeysEnum.STARTDATE.getKey())) {
	        return "Start date and time for retrieving events " + ReactorKeysEnum.STARTDATE.getKey();
	    } else if (key.equals(ReactorKeysEnum.ENDDATE.getKey())) {
	        return "End date and time for retrieving events " + ReactorKeysEnum.ENDDATE.getKey();
	    }
	    return super.getDescriptionForKey(key);
	}
}
