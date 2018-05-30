package prerna.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SemossDate {

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
			return genDateObjContainingSlash(input);
		} else if(!containsAlpha && input.contains("-")) {
			return genDateObjContainingDash(input);
		} else {
			// this is checking that it doesn't only contain numbers and / and -
			return genDateObjContainingLetters(input);
		}
	}

	/**
	 * Try to match with inputs that contain a /
	 * @param input
	 * @return
	 */
	private static SemossDate genDateObjContainingSlash(String input) {
		String[] dateMatches = new String[]{

				/*
				 * Block for month, day, year
				 */

				"[0-1][0-2]/[0-3][0-9]/[0-9]{4}",	// this matches MM/dd/yyyy where M > 10
				"[0][0-9]/[0-3][0-9]/[0-9]{4}",	// this matches MM/dd/yyyy where M < 9 and start 0
				"[0-9]/[0-3][0-9]/[0-9]{4}",	// this matches M/dd/yyyy where M < 9 and no start 0
				"[0-9]/[0-9]/[0-9]{4}",	// this matches M/d/yyyy 
				"[0-1][0-2]/[0-9]/[0-9]{4}",	// this matches MM/d/yyyy where M > 10 and d has no start 0

				// same as above but only 2 y
				"[0-1][0-2]/[0-3][0-9]/[0-9][0-9]",	// this matches MM/dd/yy where M > 10
				"[0][0-9]/[0-3][0-9]/[0-9][0-9]",	// this matches MM/dd/yy where M < 9 and start 0
				"[0-9]/[0-3][0-9]/[0-9][0-9]",	// this matches MM/dd/yy where M < 9 and no start 0
				"[0-9]/[0-9]/[0-9][0-9]",	// this matches M/dd/yy where M < 9 and no start 0 and d < 9 no start 0
				"[0-1][0-2]/[0-9]/[0-9][0-9]",	// this matches MM/d/yy where M > 10 and d has no start 0

				/*
				 * Block for day, month, year
				 */

				// if the person starts with dd
				"[0-3][0-9]/[0-1][0-2]/[0-9]{4}",	// this matches dd/MM/yyyy where M > 10
				"[0-3][0-9]/[0][0-2]/[0-9]{4}",	// this matches dd/MM/yyyy where M < 9 and start 0
				"[0-3][0-9]/[0-9]/[0-9]{4}",	// this matches dd/MM/yyyy where M < 9 and no start 0

				// same as above but only 2 y
				"[0-3][0-9]/[0-1][0-2]/[0-9][0-9]",	// this matches dd/MM/yy where M > 10
				"[0-3][0-9]/[0][0-2]/[0-9][0-9]",	// this matches dd/MM/yy where M < 9 and start 0
				"[0-3][0-9]/[0-9]/[0-9][0-9]",	// this matches dd/MM/yy where M < 9 and no start 0

				/*
				 * Block for year, month, day
				 */

				"[0-9]{4}/[0-1][0-2]/[0-3][0-9]",	// this matches yyyy/MM/dd where M > 10
				"[0-9]{4}/[0][0-9]/[0-3][0-9]",	// this matches yyyy/MM/dd where M < 9 and start 0
				"[0-9]{4}/[0-9]/[0-3][0-9]",	// this matches yyyy/M/dd where M < 9 and no start 0
				"[0-9]{4}/[0-9]/[0-9]",	// this matches yyyy/M/d 
				"[0-9]{4}/[0-1][0-2]/[0-9]",	// this matches yyyy/MM/d where M > 10 and d has no start 0

				// same as above but only 2 y
				"[0-9][0-9]/[0-1][0-2]/[0-3][0-9]",	// this matches yy/MM/dd where M > 10
				"[0-9][0-9]/[0][0-9]/[0-3][0-9]",	// this matches yy/MM/dd where M < 9 and start 0
				"[0-9][0-9]/[0-9]/[0-3][0-9]",	// this matches yy/M/dd where M < 9 and no start 0
				"[0-9][0-9]/[0-9]/[0-9]",	// this matches yy/M/dd where M < 9 and no start 0 and d < 9 no start 0
				"[0-9][0-9]/[0-1][0-2]/[0-9]",	// this matches yy/MM/d where M > 10 and d has no start 0
				
				/*
				 * Block for month, day
				 */
				
				"[0-1][0-2]/[0-3][0-9]",	// this matches MM/dd where M > 10 and d > 9
				"[0][0-9]/[0-3][0-9]",	// this matches MM/dd where M < 9 and start 0
				"[0-1][0-2]/[0-9]",	// this matches MM/dd where M > 10 and d < 10
				"[0][0-9]/[0-9]",	// this matches MM/d where M < 9 and start 0 and d < 9
				"[0-9]/[0-9]",	// this matches M/d where M < 9 and d < 9
				
				/*
				 * Block for day, month
				 */
				
				"[0-3][0-9]/[0-1][0-2]",	// this matches dd/MM where M > 10 and d > 9
				"[0-3][0-9]/[0][0-9]",	// this matches dd/MM where M < 9 and start 0
				"[0-9]/[0-1][0-2]",	// this matches dd/MM where M > 10 and d < 10
				"[0-9]/[0][0-9]",	// this matches d/MM where M < 9 and start 0 and d < 9
		};

		// matches index with above
		String[] dateFormats = new String[]{

				/*
				 * Block for month, day, year
				 */

				"MM/dd/yyyy",
				"MM/dd/yyyy",
				"M/dd/yyyy",
				"M/d/yyyy",
				"MM/d/yyyy",

				// same as above but only 2 y
				"MM/dd/yy",
				"MM/dd/yy",
				"M/dd/yy",
				"M/d/yy",
				"MM/d/yy",

				/*
				 * Block for day, month, year
				 */

				// same as above but with start dd
				"dd/MM/yyyy",
				"dd/MM/yyyy",
				"dd/M/yyyy",

				// same as above but only 2 y
				"dd/MM/yy",
				"dd/MM/yy",
				"dd/M/yy",

				/*
				 * Block for year, month, day
				 */

				"yyyy/MM/dd",
				"yyyy/MM/dd",
				"yyyy/M/dd",
				"yyyy/M/d",
				"yyyy/MM/d",

				// same as above but only 2 y
				"yy/MM/dd",
				"yy/MM/dd",
				"yy/M/dd",
				"yy/M/d",
				"yy/MM/d",
				
				/*
				 * Block for month, day
				 */
				
				"MM/dd",
				"MM/dd",
				"MM/d",
				"MM/d",
				"M/d",
				
				/*
				 * Block for day, month
				 */
				
				"dd/MM",
				"dd/MM",
				"d/MM",
				"d/MM",
		};

		SemossDate semossdate = null;
		int numFormats = dateMatches.length;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			Pattern p = Pattern.compile(dateMatches[i]);
			Matcher m = p.matcher(input);
			if(m.matches()) {
				// yay! we found a match
				semossdate = new SemossDate(input, dateFormats[i]);
				break FIND_DATE;
			}
		}

		return semossdate;
	}

	/**
	 * Try to match with inputs that contain a -
	 * @param input
	 * @return
	 */
	private static SemossDate genDateObjContainingDash(String input) {
		String[] dateMatches = new String[]{
				/*
				 * Block for month, day, year
				 */

				"[0-1][0-2]-[0-3][0-9]-[0-9]{4}",	// this matches MM-dd-yyyy where M > 10
				"[0][0-9]-[0-3][0-9]-[0-9]{4}",	// this matches MM-dd-yyyy where M < 9 and start 0
				"[0-9]-[0-3][0-9]-[0-9]{4}",	// this matches M-dd-yyyy where M < 9 and no start 0
				"[0-9]-[0-9]-[0-9]{4}",	// this matches M-d-yyyy 
				"[0-1][0-2]-[0-9]-[0-9]{4}",	// this matches MM-d-yyyy where M > 10 and d has no start 0

				// same as above but only 2 y
				"[0-1][0-2]-[0-3][0-9]-[0-9][0-9]",	// this matches MM-dd-yy where M > 10
				"[0][0-9]-[0-3][0-9]-[0-9][0-9]",	// this matches MM-dd-yy where M < 9 and start 0
				"[0-9]-[0-3][0-9]-[0-9][0-9]",	// this matches MM-dd-yy where M < 9 and no start 0
				"[0-9]-[0-9]-[0-9][0-9]",	// this matches M-dd-yy where M < 9 and no start 0 and d < 9 no start 0
				"[0-1][0-2]-[0-9]-[0-9][0-9]",	// this matches MM-d-yy where M > 10 and d has no start 0

				/*
				 * Block for day, month, year
				 */

				// if the person starts with dd
				"[0-3][0-9]-[0-1][0-2]-[0-9]{4}",	// this matches dd-MM-yyyy where M > 10
				"[0-3][0-9]-[0][0-2]-[0-9]{4}",	// this matches dd-MM-yyyy where M < 9 and start 0
				"[0-3][0-9]-[0-9]-[0-9]{4}",	// this matches dd-MM-yyyy where M < 9 and no start 0

				// same as above but only 2 y
				"[0-3][0-9]-[0-1][0-2]-[0-9][0-9]",	// this matches dd-MM-yy where M > 10
				"[0-3][0-9]-[0][0-2]-[0-9][0-9]",	// this matches dd-MM-yy where M < 9 and start 0
				"[0-3][0-9]-[0-9]-[0-9][0-9]",	// this matches dd-MM-yy where M < 9 and no start 0

				/*
				 * Block for year, month, day
				 */

				"[0-9]{4}-[0-1][0-2]-[0-3][0-9]",	// this matches yyyy-MM-dd where M > 10
				"[0-9]{4}-[0][0-9]-[0-3][0-9]",	// this matches yyyy-MM-dd where M < 9 and start 0
				"[0-9]{4}-[0-9]-[0-3][0-9]",	// this matches yyyy-M-dd where M < 9 and no start 0
				"[0-9]{4}-[0-9]-[0-9]",	// this matches yyyy-M-d 
				"[0-9]{4}-[0-1][0-2]-[0-9]",	// this matches yyyy-MM-d where M > 10 and d has no start 0

				// same as above but only 2 y
				"[0-9][0-9]-[0-1][0-2]-[0-3][0-9]",	// this matches yy-MM-dd where M > 10
				"[0-9][0-9]-[0][0-9]-[0-3][0-9]",	// this matches yy-MM-dd where M < 9 and start 0
				"[0-9][0-9]-[0-9]-[0-3][0-9]",	// this matches yy-M-dd where M < 9 and no start 0
				"[0-9][0-9]-[0-9]-[0-9]",	// this matches yy-M-dd where M < 9 and no start 0 and d < 9 no start 0
				"[0-9][0-9]-[0-1][0-2]-[0-9]",	// this matches yy-MM-d where M > 10 and d has no start 0
				
				
				/*
				 * Block for month, day
				 */
				
				"[0-1][0-2]-[0-3][0-9]",	// this matches MM-dd where M > 10 and d > 9
				"[0][0-9]-[0-3][0-9]",	// this matches MM-dd where M < 9 and start 0
				"[0-1][0-2]-[0-9]",	// this matches MM-dd where M > 10 and d < 10
				"[0][0-9]-[0-9]",	// this matches MM-d where M < 9 and start 0 and d < 9
				"[0-9]-[0-9]",	// this matches M-d where M < 9 and d < 9
				
				/*
				 * Block for day, month
				 */
				
				"[0-3][0-9]-[0-1][0-2]",	// this matches dd-MM where M > 10 and d > 9
				"[0-3][0-9]-[0][0-9]",	// this matches dd-MM where M < 9 and start 0
				"[0-9]-[0-1][0-2]",	// this matches dd-MM where M > 10 and d < 10
				"[0-9]-[0][0-9]",	// this matches d-MM where M < 9 and start 0 and d < 9
		};

		// matches index with above
		String[] dateFormats = new String[]{

				/*
				 * Block for month, day, year
				 */

				"MM-dd-yyyy",
				"MM-dd-yyyy",
				"M-dd-yyyy",
				"M-d-yyyy",
				"MM-d-yyyy",

				// same as above but only 2 y
				"MM-dd-yy",
				"MM-dd-yy",
				"M-dd-yy",
				"M-d-yy",
				"MM-d-yy",

				/*
				 * Block for day, month, year
				 */

				// same as above but with start dd
				"dd-MM-yyyy",
				"dd-MM-yyyy",
				"dd-M-yyyy",

				// same as above but only 2 y
				"dd-MM-yy",
				"dd-MM-yy",
				"dd-M-yy",

				/*
				 * Block for year, month, day
				 */

				"yyyy-MM-dd",
				"yyyy-MM-dd",
				"yyyy-M-dd",
				"yyyy-M-d",
				"yyyy-MM-d",

				// same as above but only 2 y
				"yy-MM-dd",
				"yy-MM-dd",
				"yy-M-dd",
				"yy-M-d",
				"yy-MM-d",
				
				/*
				 * Block for month, day
				 */
				
				"MM-dd",
				"MM-dd",
				"MM-d",
				"MM-d",
				"M-d",
				
				/*
				 * Block for day, month
				 */
				
				"dd-MM",
				"dd-MM",
				"d-MM",
				"d-MM",
		};

		SemossDate semossdate = null;
		int numFormats = dateMatches.length;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			Pattern p = Pattern.compile(dateMatches[i]);
			Matcher m = p.matcher(input);
			if(m.matches()) {
				// yay! we found a match
				semossdate = new SemossDate(input, dateFormats[i]);
				break FIND_DATE;
			}
		}
		
		return semossdate;
	}

	/**
	 * Try to match when there are letters
	 * @param input
	 * @return
	 */
	private static SemossDate genDateObjContainingLetters(String input) {
		String[] dateMatches = new String[] {
				/*
				 * 12 Mar 2012
				 */
				"[0-3][0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", // this matches dd MMM yyyy
				"[0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", // this matches d MMM yyyy
				"[0-3][0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", // this matches dd MMM, yyyy
				"[0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", // this matches d MMM, yyyy

				/*
				 * 12 Mar 91
				 */
				"[0-3][0-9]\\s*[a-zA-Z]{3}\\s*[0-9][0-9]", // this matches dd MMM yy
				"[0-9]\\s*[a-zA-Z]{3}\\s*[0-9][0-9]", // this matches d MMM yy
				"[0-3][0-9]\\s*[a-zA-Z]{3},\\s*[0-9][0-9]", // this matches dd MMM, yy
				"[0-9]\\s*[a-zA-Z]{3},\\s*[0-9][0-9]", // this matches d MMM, yy
			
				/*
				 * Mar 12 2012
				 */
				"[a-zA-Z]{3}\\s*[0-3][0-9]\\s*[0-9]{4}", // this matches MMM dd yyyy
				"[a-zA-Z]{3}\\s*[0-9]\\s*[0-9]{4}", // this matches MMM d yyyy
				"[a-zA-Z]{3}\\s*[0-3][0-9],\\s*[0-9]{4}", // this matches MMM dd, yyyy
				"[a-zA-Z]{3}\\s*[0-9],\\s*[0-9]{4}", // this matches MMM d, yyyy
				
				/*
				 * Mar 12 91
				 */
				"[a-zA-Z]{3}\\s*[0-3][0-9]\\s*[0-9][0-9]", // this matches MMM dd yy
				"[a-zA-Z]{3}\\s*[0-9]\\s*[0-9][0-9]", // this matches MMM d yy
				"[a-zA-Z]{3}\\s*[0-3][0-9],\\s*[0-9][0-9]", // this matches MMM dd, yy
				"[a-zA-Z]{3}\\s*[0-9],\\s*[0-9][0-9]", // this matches MMM d, yy
				
				/*
				 * Mar 12
				 */
				"[a-zA-Z]{3}\\s*[0-3][0-9]", // this matches MMM dd
				"[a-zA-Z]{3}\\s*[0-9][0-9]", // this matches MMM d
				
				/*
				 * Mar-12
				 */
				"[a-zA-Z]{3}-[0-3][0-9]", // this matches MMM-dd
				"[a-zA-Z]{3}-[0-3][0-9]", // this matches MMM-d
				
				/*
				 * Wed, Mar 12 2015
				 */
				"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-9]\\s*[0-9]{4}", // this matches EEE, MMM d yyyy
				"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-3][0-9]\\s*[0-9]{4}", // this matches EEE, MMM dd yyyy

				// additional comma compared to above
				"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-9],\\s*[0-9]{4}", // this matches EEE, MMM d, yyyy
				"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-3][0-9],\\s*[0-9]{4}", // this matches EEE, MMM dd, yyyy

				/*
				 * Wed, 12 Mar 2015
				 */
				"[a-zA-Z]{3},\\s*[0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", // this matches EEE, d MMM yyyy
				"[a-zA-Z]{3},\\s*[0-3][0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", // this matches EEE, dd MMM yyyy

				// additional comma compared to above
				"[a-zA-Z]{3},\\s*[0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", // this matches EEE, d MMM, yyyy
				"[a-zA-Z]{3},\\s*[0-3][0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", // this matches EEE, dd MMM, yyyy
		};

		// matches index with above
		String[] dateFormats = new String[]{
				
				/*
				 * 12 Mar 2012
				 */
				"dd MMM yyyy",
				"d MMM yyyy", 
				"dd MMM, yyyy",
				"d MMM, yyyy",

				/*
				 * 12 Mar 91
				 */
				"dd MMM yy",
				"d MMM yy", 
				"dd MMM, yy",
				"d MMM, yy",
				
				/*
				 * Mar 12 2012
				 */
				"MMM dd yyyy",
				"MMM d yyyy", 
				"MMM dd, yyyy",
				"MMM d, yyyy",
				
				/*
				 * Mar 12 91
				 */
				"MMM dd yy",
				"MMM d yy", 
				"MMM dd, yy",
				"MMM d, yy",
				
				/*
				 * Mar 12
				 */
				"MMM dd",
				"MMM d",
				
				/*
				 * Mar-12
				 */
				"MMM-dd", // this matches MMM-dd
				"MMM-d", // this matches MMM-d
				
				/*
				 * Wed, Mar 12 2015
				 */
				
				"EEE, MMM d yyyy",
				"EEE, MMM dd yyyy",
				// additional comma compared to above
				"EEE, MMM d, yyyy",
				"EEE, MMM dd, yyyy",
				
				/*
				 * Wed, 12 Mar 2015
				 */
				"EEE, d MMM yyyy",
				"EEE, dd MMM yyyy",
				// additional comma compared to above
				"EEE, d MMM, yyyy",
				"EEE, dd MMM, yyyy",
		};

		SemossDate semossdate = null;
		int numFormats = dateMatches.length;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			Pattern p = Pattern.compile(dateMatches[i]);
			Matcher m = p.matcher(input);
			if(m.matches()) {
				// yay! we found a match
				semossdate = new SemossDate(input, dateFormats[i]);
				break FIND_DATE;
			}
		}
		
		if(semossdate != null) {
			return semossdate;
		}
		
		// alright
		// we have a full month spelled out
		// so lets consolidate to replace
		
		String[] months = new String[]{"January", "February", "March", "April", "May",
				"June", "July", "August", "September", "October", "November", "December"};
		
		String monthUsed = null;
		final String MONTH_REPLACEMENT = "MONTH_REPLACEMENT";
		for(String m : months) {
			input = input.replaceAll("(?i)" + Pattern.quote(m), MONTH_REPLACEMENT);
			if(input.contains(MONTH_REPLACEMENT)) {
				monthUsed = m;
				break;
			}
		}
		
		// if we didn't find a match
		// no point in continuing
		if(monthUsed == null) {
			return null;
		}
		
		dateMatches = new String[]{
			/*
			 * January 1st, 2015	
			 */
			MONTH_REPLACEMENT + "\\s*[0-9][a-zA-z]{2},\\s*[0-9]{4}", // this matches MMMM d'%s', yyyy
			MONTH_REPLACEMENT + "\\s*[0-3][0-9][a-zA-z]{2},\\s*[0-9]{4}", // this matches MMMM dd'%s', yyyy

			/*
			 * January 1, 2015	
			 */
			MONTH_REPLACEMENT + "\\s*[0-9],\\s*[0-9]{4}", // this matches MMMM d, yyyy
			MONTH_REPLACEMENT + "\\s*[0-3][0-9],\\s*[0-9]{4}", // this matches MMMM dd, yyyy
		};
		
		dateFormats = new String[]{
				/*
				 * January 1st, 2015	
				 */ 
				"MMMMM d'%s', yyyy",
				"MMMMM dd'%s', yyyy",
				/*
				 * January 1, 2015	
				 */
				"MMMMM d, yyyy",
				"MMMMM dd, yyyy",
		};
		
		
		numFormats = dateMatches.length;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			Pattern p = Pattern.compile(dateMatches[i]);
			Matcher m = p.matcher(input);
			if(m.matches()) {
				// yay! we found a match
				// get back the original date
				input = input.replace(MONTH_REPLACEMENT, monthUsed);
				semossdate = new SemossDate(input, dateFormats[i]);
				break FIND_DATE;
			}
		}
		
		return semossdate;
	}

	
	
	
	public static void main(String[] args) {
		String d = "11/5/1991";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "1/5/1991";
		System.out.println(SemossDate.genDateObj(d).testToString());
		
		d = "1/22/1991";
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
	}

}
