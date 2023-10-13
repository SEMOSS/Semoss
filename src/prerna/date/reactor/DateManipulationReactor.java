package prerna.date.reactor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.ibm.icu.util.Calendar;

import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class DateManipulationReactor extends AbstractReactor {

	private static final String DEFAULT_FORMAT = "yyyy-MM-dd";

	// how to use
	// Frame() | Select( ...... ) | Filter (DATE_COLUMN_NAME >= DateManipulation("subtract", Date(), 7, "days") ) |
	// different flows 
	// DateManipulation("add", Date(), 7, "days")
	// DateManipulation("subtract", Date(), 7, "days")
	// DateManipulation("diff", Date("2020-11-01"), Date("2020-11-12"), "days")

	public DateManipulationReactor() {
		this.keysToGet = new String[] { "type", "date", "recurrence", "timeunit" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String type = this.keyValue.get(this.keysToGet[0]);
		if(type == null) {
			return NounMetadata.getErrorNounMessage("Must provide the 'type' of date manipulation");
		}
		return performManip(type);
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

		if (this.keyValue.containsKey(this.keysToGet[3])) {
			timeunit = this.keyValue.get(this.keysToGet[3]);
		}
		
		if(startingDate == null) {
			throw new SemossPixelException("Starting Date must be set");
		}

		// based on the type of operation we will do the specific date manipulations here
		if (type.equalsIgnoreCase("add") || type.equalsIgnoreCase("addition")) {
			// integer check
			if (isInteger(recurrence)) {
				recurranceInt = Integer.parseInt(recurrence);
			}
			// get calendar object
			// perform manipulation
			// return a date
			Calendar c = Calendar.getInstance();
			c.setTime(startingDate.getDate());
			Date d = dateTimeAddition(c.getTime(), recurranceInt, timeunit);
			c.setTime(d);
			dateToSendBack = new SemossDate(c.getTime(), DEFAULT_FORMAT);
			return new NounMetadata(dateToSendBack, PixelDataType.CONST_DATE);

		} else if (type.equalsIgnoreCase("subtract") || type.equalsIgnoreCase("sub")
				|| type.equalsIgnoreCase("subtraction")) {
			// integer check
			if (isInteger(recurrence)) {
				recurranceInt = Integer.parseInt(recurrence);
			}
			Calendar c = Calendar.getInstance();
			c.setTime(startingDate.getDate());
			Date d = dateTimeAddition(c.getTime(), -recurranceInt, timeunit);
			c.setTime(d);
			dateToSendBack = new SemossDate(c.getTime(), DEFAULT_FORMAT);
			return new NounMetadata(dateToSendBack, PixelDataType.CONST_DATE);

		} else if (type.equalsIgnoreCase("diff") || type.equalsIgnoreCase("difference")) {
			compareDate = new SemossDate(recurrence, DEFAULT_FORMAT);
			Calendar c = Calendar.getInstance();
			c.setTime(startingDate.getDate());
			Calendar cLater = Calendar.getInstance();
			cLater.setTime(compareDate.getDate());
			long l = totalTimeUnitsBetween(c.getTime(),cLater.getTime(),timeunit);
			return new NounMetadata((int)l, PixelDataType.CONST_INT);
		}

		
		return NounMetadata.getErrorNounMessage("Unknown type = '" + type + "' for the date manipulation");
	}

	private boolean isInteger(String recurrence) {
		try {
			Integer.parseInt(recurrence);
			return true;
		} catch (Exception e) {
		}
		return false;
	}
	
	private Date dateTimeAddition(Date date1, int valToAdd, String timeUnit) {
		LocalDateTime localdatetime1 = date1.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
		localdatetime1 = localdatetime1.plus(valToAdd, ChronoUnit.valueOf(timeUnit.toUpperCase()));
	    return Date.from(localdatetime1.atZone(ZoneId.systemDefault()).toInstant());
	}
	
	private long totalTimeUnitsBetween(Date dateBefore, Date dateAfter, String timeUnit) {
		try {
			LocalDate localdate1 = dateBefore.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			LocalDate localdate2 = dateAfter.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
			return ChronoUnit.valueOf(timeUnit.toUpperCase()).between(localdate1, localdate2);
		} catch (Exception ex) { // for time units smaller than days time-units
			long diffInMillies = dateAfter.getTime() - dateBefore.getTime();
			return TimeUnit.valueOf(timeUnit.toUpperCase()).convert(diffInMillies, TimeUnit.MILLISECONDS);
		}
	}
	
	private SemossDataType determineReturnOutput() {
		organizeKeys();
		String type = this.keyValue.get(this.keysToGet[0]);
		if(type == null) {
			throw new IllegalArgumentException("Must provide the 'type' of date manipulation");
		}
		
		if (type.equalsIgnoreCase("add") || type.equalsIgnoreCase("addition")
				|| type.equalsIgnoreCase("subtract") || type.equalsIgnoreCase("sub")
						|| type.equalsIgnoreCase("subtraction")) {
			return SemossDataType.DATE;
		} else if (type.equalsIgnoreCase("diff") || type.equalsIgnoreCase("difference")) {
			return SemossDataType.INT;
		}
		
		throw new IllegalArgumentException("Unknown type = '" + type + "' for the date manipulation");
	}
	
	@Override
	public boolean canMergeIntoQs() {
		return true;
	}
	
	@Override
	public Map<String, Object> mergeIntoQsMetadata() {
		Map<String, Object> qsMergeMetadata = new HashMap<>();
		qsMergeMetadata.put(MERGE_INTO_QS_FORMAT, MERGE_INTO_QS_FORMAT_SCALAR);
		qsMergeMetadata.put(MERGE_INTO_QS_DATATYPE, determineReturnOutput() );
		return qsMergeMetadata;
	}
	
}
