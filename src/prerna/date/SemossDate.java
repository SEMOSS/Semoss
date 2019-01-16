package prerna.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SemossDate {

	private static final Logger LOGGER = LogManager.getLogger(SemossDate.class);
	
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

	private transient Date date;

	/*
	 * This is just a basic wrapper around a date
	 * so we can store the pattern with the date
	 */

	/**
	 * Default date with enforced format
	 * @param date
	 */
	public SemossDate(Date date) {
		this(date, "yyyy-MM-dd");
	}

	/**
	 * Default date with format
	 * @param date
	 */
	public SemossDate(Date date, String pattern) {
		this.date = date;
		this.pattern = pattern;
		getFormattedDate();
	}

	/**
	 * String date + format to parse
	 * @param date
	 */
	public SemossDate(String dateVal, String pattern) {
		this.strDate = dateVal;
		this.pattern = pattern;
		getDate();
	}
	
	/**
	 * Date from time in ms
	 * @param time
	 */
	public SemossDate(Long time) {
		this.date = new Date(time);
		Calendar c = Calendar.getInstance();
		c.setTime(this.date);
		if(c.get(Calendar.HOUR) > 0
				|| c.get(Calendar.MINUTE) > 0
				|| c.get(Calendar.SECOND) > 0 ) {
			// we have a time stamp... do we have milliseconds?
			if(c.get(Calendar.MILLISECOND) > 0) {
				this.pattern = "yyyy-MM-dd HH:mm:ss";
			} else {
				this.pattern = "yyyy-MM-dd HH:mm:ss.S";
			}
		} else {
			this.pattern = "yyyy-MM-dd";
		}
		getFormattedDate();
	}
	
	/**
	 * Date from time in ms with enforcement on timestamp
	 * @param time
	 * @param timestamp
	 */
	public SemossDate(Long time, boolean timestamp) {
		this.date = new Date(time);
		Calendar c = Calendar.getInstance();
		c.setTime(this.date);
		if(timestamp || (c.get(Calendar.HOUR) > 0
				|| c.get(Calendar.MINUTE) > 0
				|| c.get(Calendar.SECOND) > 0 )) {
			// we have a time stamp... do we have milliseconds?
			if(c.get(Calendar.MILLISECOND) > 0) {
				this.pattern = "yyyy-MM-dd HH:mm:ss.S";
			} else {
				this.pattern = "yyyy-MM-dd HH:mm:ss";
			}
		} else {
			this.pattern = "yyyy-MM-dd";
		}
		getFormattedDate();
	}
	
	/**
	 * Date from time in ms with format
	 * @param time
	 * @param timestamp
	 */
	public SemossDate(Long time, String pattern) {
		this.date = new Date(time);
		this.pattern = pattern;
		getFormattedDate();
	}

	public String getPattern() {
		return this.pattern;
	}

	/**
	 * Get the string version of the date
	 * @return
	 */
	public String getFormattedDate() {
		if(this.strDate == null) {
			SimpleDateFormat formatter = new SimpleDateFormat(this.pattern);
			this.strDate = formatter.format(this.date);
		}
		return this.strDate;
	}

	/**
	 * Get the formal date object
	 * @return
	 */
	public Date getDate() {
		if(this.date == null) {
			if(this.strDate == null || this.strDate.isEmpty()) {
				// do not even attempt if empty or null
				return null;
			}
			SimpleDateFormat formatter = new SimpleDateFormat(this.pattern);
			try {
				this.date = formatter.parse(this.strDate);
			} catch (ParseException e) {
				LOGGER.error("Could not parse the date " + this.strDate + " with the format " + formatter.toPattern());
			}
		}
		return this.date;
	}
	
	/**
	 * Get the date in a requested format
	 * @param requestedPattern
	 * @return
	 */
	public String getFormatted(String requestedPattern) {
		Date date = getDate();
		SimpleDateFormat formatter = new SimpleDateFormat(requestedPattern);
		return formatter.format(date);
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
	public static Long getTimeForDate(String strInput, String format) {
		SemossDate dateValue = null;
		if(format != null && !format.isEmpty()) {
			dateValue = new SemossDate(strInput, format);
		} else {
			dateValue = SemossDate.genDateObj(strInput);
		}
		
		return getTimeForDate(dateValue);
	}
	
	/**
	 * Try to parse the date and get the time for it
	 * @param strInput
	 * @return
	 */
	public static Long getTimeForDate(String strInput) {
		SemossDate d = genDateObj(strInput);
		if(d != null) {
			if(d.getDate() != null) {
				return d.getDate().getTime();
			}
		}
		
		return null;
	}
	
	/**
	 * Get the date and get the time for it
	 * @param d
	 * @return
	 */
	public static Long getTimeForDate(SemossDate d) {
		if(d != null) {
			if(d.getDate() != null) {
				return d.getDate().getTime();
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
	public static Long getTimeForTimestamp(String strInput, String format) {
		SemossDate dateValue = null;
		if(format != null && !format.isEmpty()) {
			dateValue = new SemossDate(strInput, format);
		} else {
			dateValue = SemossDate.genTimeStampDateObj(strInput);
		}
		
		return getTimeForTimestamp(dateValue);
	}
	
	/**
	 * Try to parse the date and get the time for it
	 * @param strInput
	 * @return
	 */
	public static Long getTimeForTimestamp(String strInput) {
		SemossDate d = genDateObj(strInput);
		if(d != null) {
			if(d.getDate() != null) {
				return d.getDate().getTime();
			}
		}
		
		return null;
	}
	
	/**
	 * Get the date and get the time for it
	 * @param d
	 * @return
	 */
	public static Long getTimeForTimestamp(SemossDate d) {
		if(d != null) {
			if(d.getDate() != null) {
				return d.getDate().getTime();
			}
		}
		
		return null;
	}
	

	/**
	 * Method to get a semoss date from string input
	 * @param input
	 * @return
	 */
	public static SemossDate genDateObj(String input) {
		if(input == null) {
			return null;
		}
		input = input.trim();

		// this does a check for anything that 
		// number, a slash, or a dash
		// not exactly contians alpha, but most likely...
		boolean containsAlpha = !input.matches("[0-9/-]+");
		
		if(!containsAlpha && input.contains("/")) {
			return testCombinations(input, datesWithSlash);
		} else if(!containsAlpha && input.contains("-")) {
			return testCombinations(input, datesWithDash);
		} else {
			// this is checking that it doesn't only contain numbers and / and -
			return testCombinations(input, datesWithLetters);
		}
	}
	
	/**
	 * Method to get a semoss date from string input
	 * @param input
	 * @return
	 */
	public static SemossDate genTimeStampDateObj(String input) {
		if(input == null) {
			return null;
		}
		input = input.trim();

		// this does a check for anything that 
		// number, a slash, or a dash
		// not exactly contians alpha, but most likely...
		boolean containsAlpha = !input.matches("[0-9/-:.\\s]+");
		
		if(!containsAlpha && input.contains("/")) {
			return testCombinations(input, timeStampsWithSlash);
		} else if(!containsAlpha && input.contains("-")) {
			return testCombinations(input, timeStampsWithDash);
		}
		
		return null;
	}
	
	
	/**
	 * Try to match with inputs that contain a /
	 * @param input
	 * @return
	 */
	private static SemossDate testCombinations(String input, List<String[]> dateMatches) {
		SemossDate semossdate = null;
		int numFormats = dateMatches.size();;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			String[] match = dateMatches.get(i);
			Pattern p = Pattern.compile(match[0]);
			Matcher m = p.matcher(input);
			if(m.matches()) {
				// yay! we found a match
				semossdate = new SemossDate(input, match[1]);
				break FIND_DATE;
			}
		}

		return semossdate;
	}

	public static void main(String[] args) throws Exception {
		Object d2 = null;
		if(d2 instanceof SemossDate) {
			System.out.println("no way");
		} else {
			System.out.println("didn't break");
		}
		
		String d = "11/5/1991";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "1/5/1991";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "01/22/1991";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "13/12/1991";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "5/15/91";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "Jan-12";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
//		d = "January 12th, 2015";
//		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "Jan 15, 2019";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "1/1/2018";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "2018/1/1";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "2018/1/1 10:20:11";
		System.out.println(SemossDate.genTimeStampDateObj(d).testToString());
		
	}

}
