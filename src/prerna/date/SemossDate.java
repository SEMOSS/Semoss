package prerna.date;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

public class SemossDate implements Comparable<SemossDate>, Serializable {

	private static final Logger classLogger = LogManager.getLogger(SemossDate.class);

	/*
	 * So we do not recalculate all these combinations every time
	 */
	private static transient List<String[]> datesWithSlash;
	private static transient List<String[]> datesWithDash;
	private static transient List<String[]> datesWithLetters;
	private static transient List<String[]> timeStampsWithSlash;
	private static transient List<String[]> timeStampsWithDash;

	static {
		datesWithSlash = DatePatternGenerator.getBasicDateFormats("/");
		datesWithDash = DatePatternGenerator.getBasicDateFormats("-");
		datesWithLetters = DatePatternGenerator.getPartialMonthDateFormats();
		timeStampsWithSlash = DatePatternGenerator.getDateTimeFormats("/", ":");
		timeStampsWithDash = DatePatternGenerator.getDateTimeFormats("-", ":");
	}

	private String strDate;
	private String pattern;
	private ZoneId zoneId;

	private transient ZonedDateTime zdt;

	/*
	 * This is just a basic wrapper around a date
	 * so we can store the pattern with the date
	 */
	
	/**
	 * 
	 * @param d
	 */
	@Deprecated
	public SemossDate(Date d) {
		this(Instant.ofEpochMilli(d.getTime()), ZoneId.of(Utility.getApplicationZoneId()));
	}

	/**
	 * 
	 * @param dateVal
	 * @param pattern
	 */
	@Deprecated
	public SemossDate(String dateVal, String pattern) {
		this(dateVal, pattern, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * 
	 * @param d
	 * @param pattern
	 */
	@Deprecated
	public SemossDate(Date d, String pattern) {
		this(Instant.ofEpochMilli(d.getTime()), ZoneId.of(Utility.getApplicationZoneId()), pattern);
	}
	
	/**
	 * 
	 * @param instant
	 */
	@Deprecated
	public SemossDate(Instant instant) {
		this(instant, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * 
	 * @param time
	 */
	@Deprecated
	public SemossDate(Long time) {
		this(Instant.ofEpochMilli(time), ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * 
	 * @param time
	 * @param timestamp
	 */
	@Deprecated
	public SemossDate(Long time, boolean timestamp) {
		this(time, timestamp, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * 
	 * @param time
	 * @param pattern
	 */
	@Deprecated
	public SemossDate(Long time, String pattern) {
		this(time, pattern, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * Date from localDate
	 * @param localDate
	 */
	@Deprecated
	public SemossDate(LocalDate localDate) {
		this(localDate, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * Date from localDateTime
	 * @param localDateTime
	 */
	@Deprecated
	public SemossDate(LocalDateTime localDateTime) {
		this(localDateTime, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * 
	 * @param d
	 * @param pattern
	 * @param zoneId
	 */
	public SemossDate(Date d, String pattern, ZoneId zoneId) {
		this(Instant.ofEpochMilli(d.getTime()), zoneId, pattern);
	}
	
	/**
	 * String date + format to parse
	 * @param date
	 */
	public SemossDate(String dateVal, String pattern, ZoneId zoneId) {
		this.strDate = dateVal;
		this.pattern = pattern;
		if(zoneId == null) {
			classLogger.debug("Semoss Date being created without having a valid zone id");
			zoneId = ZoneId.of(Utility.getApplicationZoneId());
		}
		this.zoneId = zoneId;
		getZonedDateTime();
	}
	
	/**
	 * 
	 * @param instant
	 * @param zoneId
	 */
	public SemossDate(Instant instant, ZoneId zoneId) {
		if(zoneId == null) {
			classLogger.debug("Semoss Date being created without having a valid zone id");
			zoneId = ZoneId.of(Utility.getApplicationZoneId());
		}
		this.zoneId = zoneId;
		this.zdt = ZonedDateTime.ofInstant(instant, zoneId);
		if(this.zdt.getHour() == 0
				&& this.zdt.getMinute() == 0
				&& this.zdt.getSecond() == 0) {
			this.pattern = "yyyy-MM-dd";
		} else {
			this.pattern = "yyyy-MM-dd HH:mm:ss";
		}
	}
	
	/**
	 * 
	 * @param instant
	 * @param zoneId
	 * @param pattern
	 */
	public SemossDate(Instant instant, ZoneId zoneId, String pattern) {
		if(zoneId == null) {
			classLogger.debug("Semoss Date being created without having a valid zone id");
			zoneId = ZoneId.of(Utility.getApplicationZoneId());
		}
		this.zoneId = zoneId;
		this.zdt = ZonedDateTime.ofInstant(instant, zoneId);
		this.pattern = pattern;
	}
	
	/**
	 * 
	 * @param dbTimestamp
	 * @param zoneId
	 * @param pattern
	 */
	public SemossDate(java.sql.Timestamp dbTimestamp, ZoneId zoneId, String pattern) {
		if(zoneId == null) {
			classLogger.debug("Semoss Date being created without having a valid zone id");
			zoneId = ZoneId.of(Utility.getApplicationZoneId());
		}
		// assume the timestamp is at the offset of this zoneId
		ZoneOffset zoneOffset = ZonedDateTime.now(zoneId).getOffset();
        Instant instant = dbTimestamp.toLocalDateTime().toInstant(zoneOffset);
        this.zdt = ZonedDateTime.ofInstant(instant, zoneId);
		this.pattern = pattern;
	}
	
	/**
	 * 
	 * @param time
	 * @param zoneId
	 */
	public SemossDate(Long time, ZoneId zoneId) {
		this(Instant.ofEpochMilli(time), zoneId);
	}

	/**
	 * 
	 * @param time
	 * @param timestamp
	 * @param zoneId
	 */
	public SemossDate(Long time, boolean timestamp, ZoneId zoneId) {
		if(zoneId == null) {
			classLogger.debug("Semoss Date being created without having a valid zone id");
			zoneId = ZoneId.of(Utility.getApplicationZoneId());
		}
		this.zoneId = zoneId;
		this.zdt = Instant.ofEpochMilli(time).atZone(zoneId);
		if(timestamp) {
			this.pattern = "yyyy-MM-dd HH:mm:ss";
		} else {
			this.pattern = "yyyy-MM-dd";
		}
	}
	
	/**
	 * 
	 * @param time
	 * @param pattern
	 * @param zoneId
	 */
	public SemossDate(Long time, String pattern, ZoneId zoneId) {
		if(zoneId == null) {
			classLogger.debug("Semoss Date being created without having a valid zone id");
			zoneId = ZoneId.of(Utility.getApplicationZoneId());
		}
		this.zoneId = zoneId;
		this.zdt = Instant.ofEpochMilli(time).atZone(zoneId);
		this.pattern = pattern;
		getFormattedDate();
	}

	/**
	 * 
	 * @param localDate
	 * @param zoneId
	 */
	public SemossDate(LocalDate localDate, ZoneId zoneId) {
		if(zoneId == null) {
			classLogger.debug("Semoss Date being created without having a valid zone id");
			zoneId = ZoneId.of(Utility.getApplicationZoneId());
		}
		this.zoneId = zoneId;
		this.zdt = localDate.atStartOfDay(zoneId);
		this.pattern = "yyyy-MM-dd";
	}

	/**
	 * 
	 * @param localDateTime
	 * @param zoneId
	 */
	public SemossDate(LocalDateTime localDateTime, ZoneId zoneId) {
		if(zoneId == null) {
			classLogger.debug("Semoss Date being created without having a valid zone id");
			zoneId = ZoneId.of(Utility.getApplicationZoneId());
		}
		this.zoneId = zoneId;
		this.zdt = localDateTime.atZone(zoneId);
		this.pattern = "yyyy-MM-dd HH:mm:ss";
	}

	/**
	 * Date from localDateTime
	 * @param localDateTime
	 */
	public SemossDate(ZonedDateTime zonedDateTime) {
		this(zonedDateTime, "yyyy-MM-dd HH:mm:ss");
	}
	
	/**
	 * Date from localDateTime
	 * @param localDateTime
	 */
	public SemossDate(ZonedDateTime zonedDateTime, String pattern) {
		this.zdt = zonedDateTime;
		this.zoneId = this.zdt.getZone();
		this.pattern = pattern;
	}

	/**
	 * 
	 * @return
	 */
	public String getPattern() {
		return this.pattern;
	}

	/**
	 * Get the string version of the date
	 * @return
	 */
	public String getFormattedDate() {
		if(this.strDate == null) {
			getZonedDateTime();
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(this.pattern, Locale.ENGLISH);
			this.strDate = zdt.format(formatter);
		}
		return this.strDate;
	}

	/**
	 * Get the formal date object
	 * @return
	 */
	public ZonedDateTime getZonedDateTime() {
		if(this.zdt == null) {
			if(this.strDate == null || this.strDate.isEmpty()) {
				// do not even attempt if empty or null
				return null;
			}
			try {
				DateTimeFormatter formatter = new DateTimeFormatterBuilder()
						.appendPattern(this.pattern)
						.parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
		                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
		                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
						.toFormatter(Locale.ENGLISH);
				LocalDateTime localDateTime = LocalDateTime.parse(this.strDate, formatter);

				this.zdt = localDateTime.atZone(this.zoneId);
			} catch (Exception e) {
				classLogger.warn("Could not parse the date " + Utility.cleanLogString(this.strDate) + " with the format " + this.pattern);
			}
		}
		return this.zdt;
	}
	
	/**
	 * 
	 * @return
	 */
	@Deprecated
	public Date getDate() {
		if(getZonedDateTime() == null) {
			return null;
		}
		return Date.from(zdt.toInstant());
	}

	/**
	 * 
	 * @return
	 */
	public LocalDateTime getLocalDateTime() {
		if(getZonedDateTime() == null) {
			return null;
		}
		return zdt.toLocalDateTime();
	}

	/**
	 * 
	 * @return
	 */
	public LocalDate getLocalDate() {
		if(getZonedDateTime() == null) {
			return null;
		}
		return zdt.toLocalDate();
	}

	/**
	 * Get the date in a requested format
	 * @param requestedPattern
	 * @return
	 */
	public String getFormatted(String requestedPattern) {
		if(getZonedDateTime() == null) {
			return null;
		}
		
		DateTimeFormatter formatter = new DateTimeFormatterBuilder()
				.appendPattern(requestedPattern)
				.parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
				.toFormatter(Locale.ENGLISH);
        return getZonedDateTime().format(formatter);
	}

	/**
	 * Determine if this has a time portion
	 * @return
	 */
	public boolean patternHasTime() {
		if(this.pattern.contains("H") ||
				this.pattern.contains("m") ||
				this.pattern.contains("s")) {
			return true;
		}
		return false;
	}
	
	/**
	 * Use patternHasTime() instead of this method
	 * @return
	 */
	@Deprecated
	public boolean hasTime() {
		return patternHasTime();
	}

	/**
	 * 
	 * @return
	 */
	public boolean dateHasTimeNotZero() {
		if(getZonedDateTime() == null) {
			return false;
		}
		return this.zdt.getHour() != 0
				&& this.zdt.getMinute() != 0
				&& this.zdt.getSecond() != 0;
	}

	@Override
	public String toString() {
		return getFormattedDate();
	}

	/**
	 * Using this to test the string value + pattern
	 * @return
	 */
	public String testToString() {
		return this.strDate + " ::: " + this.pattern;
	}


	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////
	//////////////////////////////////////////////////////

	/*
	 * Static methods for manipulation
	 */

	/**
	 * Try to prase the date and get the time for it
	 * @param strInput
	 * @param format
	 * @return
	 */
	@Deprecated
	public static Long getTimeForDate(String strInput, String format) {
		classLogger.debug("Semoss Date being created without having a valid zone id");
		return getTimeForDate(strInput, format, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * 
	 * @param strInput
	 * @param format
	 * @param zoneId
	 * @return
	 */
	@Deprecated
	public static Long getTimeForDate(String strInput, String format, ZoneId zoneId) {
		SemossDate dateValue = null;
		if(format != null && !format.isEmpty()) {
			dateValue = new SemossDate(strInput, format, zoneId);
		} else {
			dateValue = SemossDate.genDateObj(strInput, zoneId);
		}

		return getTimeForDate(dateValue);
	}

	/**
	 * Try to parse the date and get the time for it
	 * @param strInput
	 * @return
	 */
	@Deprecated
	public static Long getTimeForDate(String strInput) {
		classLogger.debug("Semoss Date being created without having a valid zone id");
		return getTimeForDate(strInput, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * 
	 * @param strInput
	 * @param zoneId
	 * @return
	 */
	@Deprecated
	public static Long getTimeForDate(String strInput, ZoneId zoneId) {
		SemossDate d = genDateObj(strInput, zoneId);
		if(d != null) {
			if(d.getZonedDateTime() != null) {
				return d.getZonedDateTime().toInstant().toEpochMilli();
			}
		}

		return null;
	}

	/**
	 * Get the date and get the time for it
	 * @param d
	 * @return
	 */
	@Deprecated
	public static Long getTimeForDate(SemossDate d) {
		if(d != null) {
			if(d.getZonedDateTime() != null) {
				return d.getZonedDateTime().toInstant().toEpochMilli();
			}
		}

		return null;
	}

	/**
	 * Try to prase the timestamp and get the time for it
	 * @param strInput
	 * @param format
	 * @return
	 */
	@Deprecated
	public static Long getTimeForTimestamp(String strInput, String format) {
		classLogger.debug("Semoss Date being created without having a valid zone id");
		return getTimeForTimestamp(strInput, format, ZoneId.of(Utility.getApplicationZoneId()));
	}

	/**
	 * 
	 * @param strInput
	 * @param format
	 * @param zoneId
	 * @return
	 */
	@Deprecated
	public static Long getTimeForTimestamp(String strInput, String format, ZoneId zoneId) {
		SemossDate dateValue = null;
		if(format != null && !format.isEmpty()) {
			dateValue = new SemossDate(strInput, format);
		} else {
			dateValue = SemossDate.genTimeStampDateObj(strInput, zoneId);
		}

		return getTimeForTimestamp(dateValue);
	}
	
	/**
	 * Try to parse the date and get the time for it
	 * @param strInput
	 * @return
	 */
	@Deprecated
	public static Long getTimeForTimestamp(String strInput) {
		classLogger.debug("Semoss Date being created without having a valid zone id");
		return getTimeForTimestamp(strInput, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * Try to parse the date and get the time for it
	 * @param strInput
	 * @param zoneId
	 * @return
	 */
	@Deprecated
	public static Long getTimeForTimestamp(String strInput, ZoneId zoneId) {
		SemossDate d = genDateObj(strInput, zoneId);
		if(d != null) {
			if(d.getZonedDateTime() != null) {
				return d.getZonedDateTime().toInstant().toEpochMilli();
			}
		}

		return null;
	}

	/**
	 * Get the date and get the time for it
	 * @param d
	 * @return
	 */
	@Deprecated
	public static Long getTimeForTimestamp(SemossDate d) {
		if(d != null) {
			if(d.getZonedDateTime() != null) {
				return d.getZonedDateTime().toInstant().toEpochMilli();
			}
		}

		return null;
	}

	/**
	 * Method to get a semoss date from string input
	 * @param input
	 * @return
	 */
	@Deprecated
	public static SemossDate genDateObj(String input) {
		classLogger.debug("Semoss Date being created without having a valid zone id");
		return genDateObj(input, ZoneId.of(Utility.getApplicationZoneId()));
	}

	/**
	 * Method to get a semoss date from string input
	 * @param input
	 * @return
	 */
	public static SemossDate genDateObj(String input, ZoneId zoneId) {
		if(input == null) {
			return null;
		}
		input = input.trim();

		// this does a check for anything that 
		// number, a slash, or a dash
		// not exactly contains alpha, but most likely...
		boolean containsAlpha = !input.matches("[0-9/\\-]+");

		if(!containsAlpha && (input.contains("/") && !input.startsWith("/")) ) {
			return testCombinations(input, datesWithSlash, zoneId);
		} else if(!containsAlpha && (input.contains("-") && !input.startsWith("-")) ) {
			return testCombinations(input, datesWithDash, zoneId);
		} else {
			// this is checking that it doesn't only contain numbers and / and -
			return testCombinations(input, datesWithLetters, zoneId);
		}
	}

	/**
	 * Method to get a semoss date from string input
	 * @param input
	 * @return
	 */
	@Deprecated
	public static SemossDate genTimeStampDateObj(String input) {
		classLogger.debug("Semoss Date being created without having a valid zone id");
		return genTimeStampDateObj(input, ZoneId.of(Utility.getApplicationZoneId()));
	}
	
	/**
	 * Method to get a semoss date from string input
	 * @param input
	 * @return
	 */
	public static SemossDate genTimeStampDateObj(String input, ZoneId zoneId) {
		if(input == null) {
			return null;
		}
		input = input.trim();

		// this does a check for anything that 
		// number, a slash, or a dash
		// not exactly contains alpha, but most likely...
		boolean containsAlpha = !input.matches("[0-9/\\-:.\\s]+");

		if(!containsAlpha && (input.contains("/") && !input.startsWith("/")) ) {
			return testCombinations(input, timeStampsWithSlash, zoneId);
		} else if(!containsAlpha && (input.contains("-") && !input.startsWith("-")) ) {
			return testCombinations(input, timeStampsWithDash, zoneId);
		}

		return null;
	}


	/**
	 * Try to match with inputs that contain a /
	 * @param input
	 * @return
	 */
	private static SemossDate testCombinations(String input, List<String[]> dateMatches, ZoneId zoneId) {
		SemossDate semossdate = null;
		int numFormats = dateMatches.size();;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			String[] match = dateMatches.get(i);
			Pattern p = Pattern.compile(match[0]);
			Matcher m = p.matcher(input);
			if(m.matches()) {
				// yay! we found a match
				semossdate = new SemossDate(input, match[1], zoneId);
				if(semossdate.getZonedDateTime() != null) {
					break FIND_DATE;
				}
				semossdate = null;
			}
		}

		return semossdate;
	}


	@Override
	public int compareTo(SemossDate o) {
		if(getZonedDateTime() == null) {
			return -1;
		}
		if(o.getZonedDateTime() == null) {
			return 1;
		}
		return getZonedDateTime().compareTo(o.getZonedDateTime());
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null || !(obj instanceof SemossDate)) {
			return false;
		}
		
		SemossDate o = (SemossDate) obj;
		ZonedDateTime d = o.getZonedDateTime();
		
		if(getZonedDateTime() == null && d == null) {
			return true;
		} else if(getZonedDateTime() == null || d == null) {
			return false;
		}
		
		return getZonedDateTime().equals(d);
	}
	
	@Override
	public int hashCode() {
		return getZonedDateTime().hashCode();
	}
	

	
	
	
	
//	public static void main(String[] args) throws Exception {
//		Object d2 = null;
//		if (d2 instanceof SemossDate) {
//			System.out.println("no way");
//		} else {
//			System.out.println("didn't break");
//		}
//
//		String d = "11/5/1991";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "1/5/1991";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "01/22/1991";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "13/12/1991";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "5/15/91";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "Jan-12";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		// d = "January 12th, 2015";
//		// System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "Jan 15, 2019";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "1/1/2018";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "2018/1/1";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "2018/1/1 10:20:11";
//		System.out.println(SemossDate.genTimeStampDateObj(d).testToString());
//
//		d = "2018-03-12";
//		System.out.println(SemossDate.genDateObj(d).testToString());
//
//		d = "2018-03-12 10:20:11";
//		System.out.println(SemossDate.genTimeStampDateObj(d).testToString());
//
//		d = "2019-23-12 02:12:21";
//		System.out.println(SemossDate.genTimeStampDateObj(d).testToString());
//	}

}
