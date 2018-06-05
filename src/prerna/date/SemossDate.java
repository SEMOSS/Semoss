package prerna.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SemossDate {

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
	
	private String dateVal;
	private String pattern;

	private transient Date date;

	/*
	 * This is just a basic wrapper around a date
	 * so we can store the pattern with the date
	 */

	public SemossDate(Date date) {
		this(date, "yyyy-MM-dd");
	}

	public SemossDate(Date date, String pattern) {
		this.date = date;
		this.pattern = pattern;
		getFormattedDate();
	}

	public SemossDate(String dateVal, String pattern) {
		this.dateVal = dateVal;
		this.pattern = pattern;
		getDate();
	}

	public String getPattern() {
		return this.pattern;
	}

	/**
	 * Get the string version of the date
	 * @return
	 */
	public String getFormattedDate() {
		if(this.dateVal == null) {
			SimpleDateFormat formatter = new SimpleDateFormat(this.pattern);
			this.dateVal = formatter.format(this.date);
		}
		return this.dateVal;
	}

	/**
	 * Get the formal date object
	 * @return
	 */
	public Date getDate() {
		if(this.date == null) {
			SimpleDateFormat formatter = new SimpleDateFormat(this.pattern);
			try {
				this.date = formatter.parse(this.dateVal);
			} catch (ParseException e) {
				throw new IllegalArgumentException("Could not parse the date " + this.dateVal + " with the format " + formatter.toPattern());
			}
		}
		return this.date;
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
		return this.dateVal + " ::: " + this.pattern;
	}

	/**
	 * Method to get a semoss date from string input
	 * @param input
	 * @return
	 */
	public static SemossDate genDateObj(String input) {
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
		input = input.trim();

		// this does a check for anything that 
		// number, a slash, or a dash
		// not exactly contians alpha, but most likely...
		boolean containsAlpha = !input.matches("[0-9/-:\\s]+");
		
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
