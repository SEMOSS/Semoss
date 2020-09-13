package prerna.quartz.specific.tap;

import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.quartz.SimpleTrigger;

public class CreateTriggerDetails {
	public static final int STARTYEAR = 2017;
	public static final int STARTMONTH = 5;
	public static final int STARTDAY = 22;
	public static final int REPEATMINUTE = 1;
	
	public SimpleTrigger createTrigger() {
		//get current time
		Date date = new Date();   // given date
		Calendar currentCalendar = GregorianCalendar.getInstance(); // creates a new calendar instance
		currentCalendar.setTime(date);   // assigns calendar to given date 
		int StartHour = currentCalendar.get(Calendar.HOUR_OF_DAY); // gets hour in 24h format
		int StartMinute = currentCalendar.get(Calendar.MINUTE) + 1;       // gets minute number
		
		//create the start on date time
        Calendar startCalendar = Calendar.getInstance();
        startCalendar.set(STARTYEAR, STARTMONTH, STARTDAY, StartHour, StartMinute);
		Date startDate = startCalendar.getTime();
        
        //build the trigger using constants
		SimpleTrigger trigger = (SimpleTrigger) newTrigger()
			.withIdentity("trigger1", "group1")
			.startAt(startDate)
		    .withSchedule(simpleSchedule()
		            .withIntervalInMinutes(REPEATMINUTE)
		            .repeatForever())
		    .build();

	    return trigger;
	}
}
