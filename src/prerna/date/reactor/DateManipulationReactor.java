package prerna.date.reactor;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.ibm.icu.util.Calendar;

import prerna.date.SemossDate;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DateManipulationReactor extends AbstractReactor {

	private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

	// Frame() | Select( ...... ) | Filter (DATE_COLUMN_NAME >= DateManipulation("subtract", Date(), 7, "days") ) |

	// different flows
	// DateManipulation("add", Date(), 7, "days")
	// DateManipulation("subtract", Date(), 7, "days")
	// DateManipulation("diff", Date("2020-11-01"), Date("2020-11-12"), "days")

	// "type" "date" "recurrence" "timeunit"

	public DateManipulationReactor() {
		this.keysToGet = new String[] { "type", "date", "recurrence", "timeunit" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		NounMetadata retNoun = null;
		// get the type of date manipulation
		if (this.keyValue.containsKey(this.keysToGet[0])) {
			String type = this.keyValue.get(this.keysToGet[0]);
			retNoun = performManip(type);
		}
		return retNoun;
	}

	private NounMetadata performManip(String type) {

		SemossDate startingDate = null;
		SemossDate compareDate = null;
		SemossDate dateToSendBack = null;
		String recurrence = "";
		String timeunit = "";
		int recurranceInt = 0;

		// get remaining keys
		if (this.keyValue.containsKey(this.keysToGet[1])) {
			String date = this.keyValue.get(this.keysToGet[1]);
			startingDate = new SemossDate(date, DEFAULT_FORMAT);
		}

		if (this.keyValue.containsKey(this.keysToGet[2])) {
			recurrence = this.keyValue.get(this.keysToGet[2]);
		} 

		// get the type of date manipulation
		if (this.keyValue.containsKey(this.keysToGet[3])) {
			timeunit = this.keyValue.get(this.keysToGet[3]);
		}

		// based on the type of operation we will do the specific date manipulations here
		if (type.equalsIgnoreCase("add")) {
			// check integer
			if (isInteger(recurrence)) {
				recurranceInt = Integer.parseInt(recurrence);
			}
			// get calendar object
			// perform manipulation
			// return date
			Calendar c = Calendar.getInstance();
			c.setTime(startingDate.getDate());
			Date d = doDateAddition(c.getTime(), recurranceInt, TimeUnit.valueOf(timeunit.toUpperCase()),timeunit.toUpperCase());
			c.setTime(d);
			dateToSendBack = new SemossDate(c.getTime(), DEFAULT_FORMAT);
			return new NounMetadata(dateToSendBack, PixelDataType.CONST_DATE);

		} else if (type.equalsIgnoreCase("subtract")) {
			// check integer
			if (isInteger(recurrence)) {
				recurranceInt = Integer.parseInt(recurrence);
			}
			Calendar c = Calendar.getInstance();
			c.setTime(startingDate.getDate());
			Date d = doDateAddition(c.getTime(), -recurranceInt, TimeUnit.valueOf(timeunit.toUpperCase()),timeunit.toUpperCase());
			c.setTime(d);
			dateToSendBack = new SemossDate(c.getTime(), DEFAULT_FORMAT);
			return new NounMetadata(dateToSendBack, PixelDataType.CONST_DATE);

		} else if (type.equalsIgnoreCase("diff")) {
			//get second date to do diff
			compareDate = new SemossDate(recurrence, DEFAULT_FORMAT);
			Calendar c = Calendar.getInstance();
			c.setTime(startingDate.getDate());
			Calendar cLater = Calendar.getInstance();
			cLater.setTime(compareDate.getDate());
			long l = getDateDiff(c.getTime(),cLater.getTime(),TimeUnit.valueOf(timeunit.toUpperCase()),timeunit.toUpperCase());
			return new NounMetadata((int)l, PixelDataType.CONST_INT);
		}
		return null;
	}

	private boolean isInteger(String recurrence) {
		try {
			Integer.parseInt(recurrence);
			return true;
		} catch (Exception e) {
		}
		return false;
	}
	
	private Date doDateAddition(Date date1, int valToAdd, TimeUnit timeUnit, String timeunit) {
	    long additionInMillies = date1.getTime() + timeUnit.toMillis(timeUnit.convert((long) valToAdd,TimeUnit.valueOf(timeunit.toUpperCase())));
	    return new Date(additionInMillies);
	}
	
	private long getDateDiff(Date date1, Date date2, TimeUnit timeUnit, String timeunit) {
	    long diffInMillies = date2.getTime() - date1.getTime();
	    return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
	}
}
