package prerna.io.connector.calendar;

import java.util.*;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.*;
import com.google.api.client.util.DateTime;

public class GoogleCalendarHelper {

	public static Event createEvent(Calendar service, String summary, String location, String desc, String startdatetime,
			String enddatetime, List<String> attendeeEmails, Boolean enableVideoConferencing) throws Exception {
		Event event = new Event().setSummary(summary).setLocation(location).setDescription(desc);

		DateTime startDateTime = new DateTime(startdatetime);
		EventDateTime start = new EventDateTime().setDateTime(startDateTime).setTimeZone("Asia/Kolkata");
		event.setStart(start);

		DateTime endDateTime = new DateTime(enddatetime);
		EventDateTime end = new EventDateTime().setDateTime(endDateTime).setTimeZone("Asia/Kolkata");
		event.setEnd(end);

		List<EventAttendee> attendees = new ArrayList<>();
        for (String email : attendeeEmails) {
            attendees.add(new EventAttendee().setEmail(email));
        }
        event.setAttendees(attendees);

		EventReminder[] reminderOverrides = new EventReminder[] {
				new EventReminder().setMethod("email").setMinutes(24 * 60),
				new EventReminder().setMethod("popup").setMinutes(10), };
		Event.Reminders reminders = new Event.Reminders().setUseDefault(false)
				.setOverrides(Arrays.asList(reminderOverrides));
		event.setReminders(reminders);
		if(enableVideoConferencing) {
			ConferenceSolutionKey conferenceSolutionKey = new ConferenceSolutionKey()
		            .setType("hangoutsMeet");
		    CreateConferenceRequest createConferenceRequest = new CreateConferenceRequest()
		            .setRequestId(UUID.randomUUID().toString())
		            .setConferenceSolutionKey(conferenceSolutionKey);
		    ConferenceData conferenceData = new ConferenceData()
		            .setCreateRequest(createConferenceRequest);
		    event.setConferenceData(conferenceData);
		}
		else {
			event.setConferenceData(null);
		}
		String calendarId = "primary";
		Event createdevent = service.events().insert(calendarId, event).setConferenceDataVersion(1).execute();
		return createdevent;
	}
	
	public static Event readEvent(Calendar service, String id) throws Exception {
		
		Event event = service.events().get("primary", id).execute();
		return event;
	}
	
	public static Boolean updateEvent(Calendar service, String id, String summary, String location, String desc, String startdatetime,
	String enddatetime, List<String> attendeeEmails, Boolean enableVideoConferencing) throws Exception {
		
		try {
			Event event = service.events().get("primary", id).execute();
			event.setSummary(summary);
			event.setLocation(location);
			event.setDescription(desc);
			
			DateTime startDateTime = new DateTime(startdatetime);
			EventDateTime start = new EventDateTime().setDateTime(startDateTime).setTimeZone("Asia/Kolkata");
			event.setStart(start);

			DateTime endDateTime = new DateTime(enddatetime);
			EventDateTime end = new EventDateTime().setDateTime(endDateTime).setTimeZone("Asia/Kolkata");
			event.setEnd(end);
			
			List<EventAttendee> attendees = new ArrayList<>();
	        for (String email : attendeeEmails) {
	            attendees.add(new EventAttendee().setEmail(email));
	        }
	        event.setAttendees(attendees);
			
			if (enableVideoConferencing) {
	            ConferenceSolutionKey conferenceSolutionKey = new ConferenceSolutionKey()
	                    .setType("hangoutsMeet");
	            CreateConferenceRequest createConferenceRequest = new CreateConferenceRequest()
	                    .setRequestId(UUID.randomUUID().toString())
	                    .setConferenceSolutionKey(conferenceSolutionKey);
	            ConferenceData conferenceData = new ConferenceData()
	                    .setCreateRequest(createConferenceRequest);
	            event.setConferenceData(conferenceData);
	        } else {
	            event.setConferenceData(null);
	        }
			
			service.events().update("primary", event.getId(), event).setConferenceDataVersion(1).execute();
			System.out.println("Event with id " + id + " updated successfully");
			return true;
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static Boolean deleteEvent(Calendar service, String id) throws Exception {
		try {
			service.events().delete("primary", id).execute();
			System.out.println("Event with id " + id + " deleted successfully");
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static Event recurringEvent(Calendar service, String summary, String location, String description, String startdatetime, String enddatetime, List<String> attendeeEmails, String frequency, String Until, Boolean enableVideoConferencing) throws Exception {
		if(frequency != null) {
			frequency = frequency.trim().toUpperCase();
		}
		if (!frequency.equals("DAILY") && !frequency.equals("WEEKLY")) {
	        throw new IllegalArgumentException("Frequency must be 'DAILY' or 'WEEKLY'");
	    }
		Event event = new Event();

		event.setSummary(summary);
		event.setLocation(location);
		event.setDescription(description);
		
		List<EventAttendee> attendees = new ArrayList<>();
        for (String email : attendeeEmails) {
            attendees.add(new EventAttendee().setEmail(email));
        }
        event.setAttendees(attendees);
        if (enableVideoConferencing) {
            ConferenceSolutionKey conferenceSolutionKey = new ConferenceSolutionKey()
                    .setType("hangoutsMeet");
            CreateConferenceRequest createConferenceRequest = new CreateConferenceRequest()
                    .setRequestId(UUID.randomUUID().toString())
                    .setConferenceSolutionKey(conferenceSolutionKey);
            ConferenceData conferenceData = new ConferenceData()
                    .setCreateRequest(createConferenceRequest);
            event.setConferenceData(conferenceData);
        } else {
            event.setConferenceData(null);
        }
		DateTime start = DateTime.parseRfc3339(startdatetime);
		DateTime end = DateTime.parseRfc3339(enddatetime);
		event.setStart(new EventDateTime().setDateTime(start).setTimeZone("Asia/Kolkata"));
		event.setEnd(new EventDateTime().setDateTime(end).setTimeZone("Asia/Kolkata"));
		event.setRecurrence(Arrays.asList("RRULE:FREQ="+frequency+";UNTIL="+Until));
		Event recurringEvent = service.events().insert("primary", event).setConferenceDataVersion(1).execute();
		return recurringEvent;
	}
	
	public static Event searchEvent(Calendar service, String summary) throws Exception {
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
	                return item;
	            }
	        }

	        pageToken = events.getNextPageToken();
	        if (pageToken == null) {
	            break;
	        }
	    }

	    return null;
	}
	
	public static Boolean isUserBusy(Calendar service, String userEmail, String timeMin, String timeMax) throws Exception {
        FreeBusyRequest fbRequest = new FreeBusyRequest();
        DateTime TimeMin = new DateTime(timeMin);
        DateTime TimeMax = new DateTime(timeMax);
        fbRequest.setTimeMin(TimeMin);
        fbRequest.setTimeMax(TimeMax);

        FreeBusyRequestItem item = new FreeBusyRequestItem();
        item.setId(userEmail);
        fbRequest.setItems(Collections.singletonList(item));

        Calendar.Freebusy.Query fbQuery = service.freebusy().query(fbRequest);
        FreeBusyResponse fbResponse = fbQuery.execute();

        List<TimePeriod> busyTimes = fbResponse.getCalendars().get(userEmail).getBusy();

        return !busyTimes.isEmpty();
    }

}
