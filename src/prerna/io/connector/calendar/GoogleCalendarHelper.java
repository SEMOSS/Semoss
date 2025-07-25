package prerna.io.connector.calendar;

import java.util.*;
import com.google.api.services.calendar.Calendar;
import com.google.api.services.calendar.model.*;
import com.google.api.client.util.DateTime;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class GoogleCalendarHelper {
	
	private static final Logger classLogger = LogManager.getLogger(GoogleCalendarHelper.class);

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
	String enddatetime, List<String> attendeeEmails, String frequency, String until, Boolean enableVideoConferencing) throws Exception {
		
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
			if(frequency != null) {
				frequency = frequency.trim().toUpperCase();
				if (!frequency.equals("DAILY") && !frequency.equals("WEEKLY") && !frequency.equals("NONE")) {
				    throw new IllegalArgumentException("Frequency must be 'DAILY' or 'WEEKLY' or 'NONE'");
				}
				if(frequency.equals("NONE")) {
					event.setRecurrence(null);
				}
				else if (frequency.equals("DAILY") || frequency.equals("WEEKLY")) {
					if (until == null || until.trim().isEmpty()) {
			            throw new IllegalArgumentException("Until date must be provided for recurring events.");
			        }
					event.setRecurrence(Arrays.asList("RRULE:FREQ="+frequency+";UNTIL="+until));
				}
			}
			service.events().update("primary", event.getId(), event).setConferenceDataVersion(1).execute();
			return true;
			
		} catch (Exception e) {
			classLogger.error("Failed to update event");
			return false;
		}
	}
	
	public static Boolean deleteEvent(Calendar service, String id) throws Exception {
		try {
			service.events().delete("primary", id).execute();
			return true;
		} catch (Exception e) {
			classLogger.error("Failed to delete event");
			return false;
		}
	}
	
	public static Event recurringEvent(Calendar service, String summary, String location, String description, String startdatetime, String enddatetime, List<String> attendeeEmails, String frequency, String until, Boolean enableVideoConferencing) throws Exception {
		if (frequency == null) {
		    throw new IllegalArgumentException("Frequency must not be null and must be 'DAILY' or 'WEEKLY'");
		}
		frequency = frequency.trim().toUpperCase();
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
		event.setRecurrence(Arrays.asList("RRULE:FREQ="+frequency+";UNTIL="+until));
		Event recurringEvent = service.events().insert("primary", event).setConferenceDataVersion(1).execute();
		return recurringEvent;
	}
	
	public static Event searchEvent(Calendar service, String eventId) throws Exception {
		return service.events().get("primary", eventId).execute();
	}

}
