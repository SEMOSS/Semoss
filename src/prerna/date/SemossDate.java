package prerna.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import prerna.algorithm.api.SemossDataType;

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
		String[][] dateMatches = new String[][]{
				/*
				 * Block for month, day, year
				 */

				new String[]{"[0-1][0-2]/[0-3][0-9]/[0-9]{4}", "MM/dd/yyyy"}, 	// this matches MM/dd/yyyy where M > 10
				new String[]{"[0][0-9]/[0-3][0-9]/[0-9]{4}", "MM/dd/yyyy"},	// this matches MM/dd/yyyy where M < 9 and start 0
				new String[]{"[0-9]/[0-3][0-9]/[0-9]{4}", "M/dd/yyyy"},	// this matches M/dd/yyyy where M < 9 and no start 0
				new String[]{"[0-9]/[0-9]/[0-9]{4}", "M/d/yyyy"},	// this matches M/d/yyyy 
				new String[]{"[0-1][0-2]/[0-9]/[0-9]{4}", "MM/d/yyyy"},	// this matches MM/d/yyyy where M > 10 and d has no start 0

				// same as above but only 2 y
				new String[]{"[0-1][0-2]/[0-3][0-9]/[0-9][0-9]", "MM/dd/yy"},	// this matches MM/dd/yy where M > 10
				new String[]{"[0][0-9]/[0-3][0-9]/[0-9][0-9]", "MM/dd/yy"},	// this matches MM/dd/yy where M < 9 and start 0
				new String[]{"[0-9]/[0-3][0-9]/[0-9][0-9]", "M/dd/yy"},	// this matches MM/dd/yy where M < 9 and no start 0
				new String[]{"[0-9]/[0-9]/[0-9][0-9]", "M/d/yy"},	// this matches M/dd/yy where M < 9 and no start 0 and d < 9 no start 0
				new String[]{"[0-1][0-2]/[0-9]/[0-9][0-9]", "MM/d/yy"},	// this matches MM/d/yy where M > 10 and d has no start 0

				/*
				 * Block for day, month, year
				 */

				// if the person starts with dd
				new String[]{"[0-3][0-9]/[0-1][0-2]/[0-9]{4}", "dd/MM/yyyy"},	// this matches dd/MM/yyyy where M > 10
				new String[]{"[0-3][0-9]/[0][0-2]/[0-9]{4}", "dd/MM/yyyy"},	// this matches dd/MM/yyyy where M < 9 and start 0
				new String[]{"[0-3][0-9]/[0-9]/[0-9]{4}", "dd/M/yyyy"},	// this matches dd/MM/yyyy where M < 9 and no start 0

				// same as above but only 2 y
				new String[]{"[0-3][0-9]/[0-1][0-2]/[0-9][0-9]", "dd/MM/yy"},	// this matches dd/MM/yy where M > 10
				new String[]{"[0-3][0-9]/[0][0-2]/[0-9][0-9]", "dd/MM/yy"},	// this matches dd/MM/yy where M < 9 and start 0
				new String[]{"[0-3][0-9]/[0-9]/[0-9][0-9]", "dd/M/yy"}, // this matches dd/MM/yy where M < 9 and no start 0

				/*
				 * Block for year, month, day
				 */

				new String[]{"[0-9]{4}/[0-1][0-2]/[0-3][0-9]", "yyyy/MM/dd"},	// this matches yyyy/MM/dd where M > 10
				new String[]{"[0-9]{4}/[0][0-9]/[0-3][0-9]", "yyyy/MM/dd"},	// this matches yyyy/MM/dd where M < 9 and start 0
				new String[]{"[0-9]{4}/[0-9]/[0-3][0-9]", "yyyy/M/dd"},	// this matches yyyy/M/dd where M < 9 and no start 0
				new String[]{"[0-9]{4}/[0-9]/[0-9]", "yyyy/M/d"},	// this matches yyyy/M/d 
				new String[]{"[0-9]{4}/[0-1][0-2]/[0-9]", "yyyy/MM/d"},	// this matches yyyy/MM/d where M > 10 and d has no start 0

				// same as above but only 2 y
				new String[]{"[0-9][0-9]/[0-1][0-2]/[0-3][0-9]", "yy/MM/dd"},	// this matches yy/MM/dd where M > 10
				new String[]{"[0-9][0-9]/[0][0-9]/[0-3][0-9]", "yy/MM/dd"},	// this matches yy/MM/dd where M < 9 and start 0
				new String[]{"[0-9][0-9]/[0-9]/[0-3][0-9]", "yy/M/dd"},	// this matches yy/M/dd where M < 9 and no start 0
				new String[]{"[0-9][0-9]/[0-9]/[0-9]", "yy/M/d"},	// this matches yy/M/dd where M < 9 and no start 0 and d < 9 no start 0
				new String[]{"[0-9][0-9]/[0-1][0-2]/[0-9]",	"yy/MM/d"}, // this matches yy/MM/d where M > 10 and d has no start 0
				
				/*
				 * Block for month, day
				 */
				
				new String[]{"[0-1][0-2]/[0-3][0-9]", "MM/dd"},	// this matches MM/dd where M > 10 and d > 9
				new String[]{"[0][0-9]/[0-3][0-9]", "MM/d"}, // this matches MM/dd where M < 9 and start 0
				new String[]{"[0-1][0-2]/[0-9]", "MM/d"},	// this matches MM/dd where M > 10 and d < 10
				new String[]{"[0][0-9]/[0-9]", "MM/d"},	// this matches MM/d where M < 9 and start 0 and d < 9
				new String[]{"[0-9]/[0-9]", "M/d"},	// this matches M/d where M < 9 and d < 9
				
				/*
				 * Block for day, month
				 */
				
				new String[]{"[0-3][0-9]/[0-1][0-2]", "dd/MM"},	// this matches dd/MM where M > 10 and d > 9
				new String[]{"[0-3][0-9]/[0][0-9]", "dd/MM"}, // this matches dd/MM where M < 9 and start 0
				new String[]{"[0-9]/[0-1][0-2]", "d/MM"},	// this matches d/MM where M > 10 and d < 10
				new String[]{"[0-9]/[0][0-9]", "d/MM"}	// this matches d/MM where M < 9 and start 0 and d < 9
		};


		SemossDate semossdate = null;
		int numFormats = dateMatches.length;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			String[] match = dateMatches[i];
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

	/**
	 * Try to match with inputs that contain a -
	 * @param input
	 * @return
	 */
	private static SemossDate genDateObjContainingDash(String input) {
		String[][] dateMatches = new String[][]{
				/*
				 * Block for month, day, year
				 */

				new String[]{"[0-1][0-2]-[0-3][0-9]-[0-9]{4}", "MM-dd-yyyy"},	// this matches MM-dd-yyyy where M > 10
				new String[]{"[0][0-9]-[0-3][0-9]-[0-9]{4}", "MM-dd-yyyy"}, // this matches MM-dd-yyyy where M < 9 and start 0
				new String[]{"[0-9]-[0-3][0-9]-[0-9]{4}", "M-dd-yyyy"},	// this matches M-dd-yyyy where M < 9 and no start 0
				new String[]{"[0-9]-[0-9]-[0-9]{4}", "M-d-yyyy"},	// this matches M-d-yyyy 
				new String[]{"[0-1][0-2]-[0-9]-[0-9]{4}", "MM-d-yyyy"},	// this matches MM-d-yyyy where M > 10 and d has no start 0

				// same as above but only 2 y
				new String[]{"[0-1][0-2]-[0-3][0-9]-[0-9][0-9]", "MM-dd-yy"},	// this matches MM-dd-yy where M > 10
				new String[]{"[0][0-9]-[0-3][0-9]-[0-9][0-9]", "MM-dd-yy"},	// this matches MM-dd-yy where M < 9 and start 0
				new String[]{"[0-9]-[0-3][0-9]-[0-9][0-9]", "M-dd-yy"},	// this matches MM-dd-yy where M < 9 and no start 0
				new String[]{"[0-9]-[0-9]-[0-9][0-9]", "M-d-yy"},	// this matches M-dd-yy where M < 9 and no start 0 and d < 9 no start 0
				new String[]{"[0-1][0-2]-[0-9]-[0-9][0-9]",	"MM-d-yy"}, // this matches MM-d-yy where M > 10 and d has no start 0

				/*
				 * Block for day, month, year
				 */

				// if the person starts with dd
				new String[]{"[0-3][0-9]-[0-1][0-2]-[0-9]{4}", "dd-MM-yyyy"},	// this matches dd-MM-yyyy where M > 10
				new String[]{"[0-3][0-9]-[0][0-2]-[0-9]{4}", "dd-MM-yyyy"},	// this matches dd-MM-yyyy where M < 9 and start 0
				new String[]{"[0-3][0-9]-[0-9]-[0-9]{4}", "dd-M-yyyy"},	// this matches dd-MM-yyyy where M < 9 and no start 0

				// same as above but only 2 y
				new String[]{"[0-3][0-9]-[0-1][0-2]-[0-9][0-9]", "dd-MM-yy"},	// this matches dd-MM-yy where M > 10
				new String[]{"[0-3][0-9]-[0][0-2]-[0-9][0-9]", "dd-MM-yy"},	// this matches dd-MM-yy where M < 9 and start 0
				new String[]{"[0-3][0-9]-[0-9]-[0-9][0-9]", "dd-M-yy"},	// this matches dd-MM-yy where M < 9 and no start 0

				/*
				 * Block for year, month, day
				 */

				new String[]{"[0-9]{4}-[0-1][0-2]-[0-3][0-9]", "yyyy-MM-dd"}, // this matches yyyy-MM-dd where M > 10
				new String[]{"[0-9]{4}-[0][0-9]-[0-3][0-9]", "yyyy-MM-dd"},	// this matches yyyy-MM-dd where M < 9 and start 0
				new String[]{"[0-9]{4}-[0-9]-[0-3][0-9]", "yyyy-M-dd"}, // this matches yyyy-M-dd where M < 9 and no start 0
				new String[]{"[0-9]{4}-[0-9]-[0-9]", "yyyy-M-d"}, // this matches yyyy-M-d 
				new String[]{"[0-9]{4}-[0-1][0-2]-[0-9]", "yyyy-MM-d"}, // this matches yyyy-MM-d where M > 10 and d has no start 0

				// same as above but only 2 y
				new String[]{"[0-9][0-9]-[0-1][0-2]-[0-3][0-9]", "yy-MM-dd"}, // this matches yy-MM-dd where M > 10
				new String[]{"[0-9][0-9]-[0][0-9]-[0-3][0-9]", "yy-MM-dd"},	// this matches yy-MM-dd where M < 9 and start 0
				new String[]{"[0-9][0-9]-[0-9]-[0-3][0-9]",	"yy-M-dd"}, // this matches yy-M-dd where M < 9 and no start 0
				new String[]{"[0-9][0-9]-[0-9]-[0-9]", "yy-M-d"}, // this matches yy-M-dd where M < 9 and no start 0 and d < 9 no start 0
				new String[]{"[0-9][0-9]-[0-1][0-2]-[0-9]", "yy-MM-d"}, // this matches yy-MM-d where M > 10 and d has no start 0
				
				
				/*
				 * Block for month, day
				 */
				
				new String[]{"[0-1][0-2]-[0-3][0-9]", "MM-dd"}, // this matches MM-dd where M > 10 and d > 9
				new String[]{"[0][0-9]-[0-3][0-9]",	"MM-dd"}, // this matches MM-dd where M < 9 and start 0
				new String[]{"[0-1][0-2]-[0-9]", "MM-d"}, // this matches MM-dd where M > 10 and d < 10
				new String[]{"[0][0-9]-[0-9]", "MM-d"}, // this matches MM-d where M < 9 and start 0 and d < 9
				new String[]{"[0-9]-[0-9]",	"M-d"}, // this matches M-d where M < 9 and d < 9
				
				/*
				 * Block for day, month
				 */
				
				new String[]{"[0-3][0-9]-[0][0-9]",	"dd-MM"}, // this matches dd-MM where M < 9 and start 0
				new String[]{"[0-9]-[0-1][0-2]", "dd-MM"}, // this matches dd-MM where M > 10 and d < 10
				new String[]{"[0-9]-[0][0-9]", "dd-MM"}	// this matches d-MM where M < 9 and start 0 and d < 9
		};

		SemossDate semossdate = null;
		int numFormats = dateMatches.length;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			String[] match = dateMatches[i];
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

	/**
	 * Try to match when there are letters
	 * @param input
	 * @return
	 */
	private static SemossDate genDateObjContainingLetters(String input) {
		String[][] dateMatches = new String[][]{
				/*
				 * 12 Mar 2012
				 */
				new String[]{"[0-3][0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", "dd MMM yyyy"}, // this matches dd MMM yyyy
				new String[]{"[0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", "d MMM yyyy"}, // this matches d MMM yyyy
				new String[]{"[0-3][0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", "dd MMM, yyyy"}, // this matches dd MMM, yyyy
				new String[]{"[0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", "d MMM, yyyy"}, // this matches d MMM, yyyy

				/*
				 * 12 Mar 91
				 */
				new String[]{"[0-3][0-9]\\s*[a-zA-Z]{3}\\s*[0-9][0-9]", "dd MMM yy"}, // this matches dd MMM yy
				new String[]{"[0-9]\\s*[a-zA-Z]{3}\\s*[0-9][0-9]", "d MMM yy"}, // this matches d MMM yy
				new String[]{"[0-3][0-9]\\s*[a-zA-Z]{3},\\s*[0-9][0-9]", "dd MMM, yy"}, // this matches dd MMM, yy
				new String[]{"[0-9]\\s*[a-zA-Z]{3},\\s*[0-9][0-9]", "d MMM, yy"}, // this matches d MMM, yy
			
				/*
				 * Mar 12 2012
				 */
				new String[]{"[a-zA-Z]{3}\\s*[0-3][0-9]\\s*[0-9]{4}", "MMM dd yyyy"}, // this matches MMM dd yyyy
				new String[]{"[a-zA-Z]{3}\\s*[0-9]\\s*[0-9]{4}", "MMM d yyyy"}, // this matches MMM d yyyy
				new String[]{"[a-zA-Z]{3}\\s*[0-3][0-9],\\s*[0-9]{4}", "MMM dd, yyyy"}, // this matches MMM dd, yyyy
				new String[]{"[a-zA-Z]{3}\\s*[0-9],\\s*[0-9]{4}", "MMM d, yyyy"}, // this matches MMM d, yyyy
				
				/*
				 * Mar 12 91
				 */
				new String[]{"[a-zA-Z]{3}\\s*[0-3][0-9]\\s*[0-9][0-9]", "MMM dd yy"}, // this matches MMM dd yy
				new String[]{"[a-zA-Z]{3}\\s*[0-9]\\s*[0-9][0-9]", "MMM d yy"}, // this matches MMM d yy
				new String[]{"[a-zA-Z]{3}\\s*[0-3][0-9],\\s*[0-9][0-9]", "MMM dd, yy"}, // this matches MMM dd, yy
				new String[]{"[a-zA-Z]{3}\\s*[0-9],\\s*[0-9][0-9]", "MMM d, yy"}, // this matches MMM d, yy
				
				/*
				 * Mar 12
				 */
				new String[]{"[a-zA-Z]{3}\\s*[0-3][0-9]", "MMM dd"}, // this matches MMM dd
				new String[]{"[a-zA-Z]{3}\\s*[0-9][0-9]", "MMM d"}, // this matches MMM d
				
				/*
				 * Mar-12
				 */
				new String[]{"[a-zA-Z]{3}-[0-3][0-9]", "MMM-dd"}, // this matches MMM-dd
				new String[]{"[a-zA-Z]{3}-[0-3][0-9]", "MMM-d"}, // this matches MMM-d
				
				/*
				 * Wed, Mar 12 2015
				 */
				new String[]{"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-9]\\s*[0-9]{4}", "EEE, MMM d yyyy"}, // this matches EEE, MMM d yyyy
				new String[]{"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-3][0-9]\\s*[0-9]{4}", "EEE, MMM dd yyyy"}, // this matches EEE, MMM dd yyyy

				// additional comma compared to above
				new String[]{"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-9],\\s*[0-9]{4}", "EEE, MMM d, yyyy"}, // this matches EEE, MMM d, yyyy
				new String[]{"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-3][0-9],\\s*[0-9]{4}", "EEE, MMM dd, yyyy"}, // this matches EEE, MMM dd, yyyy

				/*
				 * Wed, 12 Mar 2015
				 */
				new String[]{"[a-zA-Z]{3},\\s*[0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", "EEE, d MMM yyyy"}, // this matches EEE, d MMM yyyy
				new String[]{"[a-zA-Z]{3},\\s*[0-3][0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", "EEE, dd MMM yyyy"}, // this matches EEE, dd MMM yyyy

				// additional comma compared to above
				new String[]{"[a-zA-Z]{3},\\s*[0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", "EEE, d MMM, yyyy"}, // this matches EEE, d MMM, yyyy
				new String[]{"[a-zA-Z]{3},\\s*[0-3][0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", "EEE, dd MMM, yyyy"}// this matches EEE, dd MMM, yyyy
		};

		SemossDate semossdate = null;
		int numFormats = dateMatches.length;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			String[] match = dateMatches[i];
			Pattern p = Pattern.compile(match[0]);
			Matcher m = p.matcher(input);
			if(m.matches()) {
				// yay! we found a match
				semossdate = new SemossDate(input, match[1]);
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
		
		dateMatches = new String[][]{
			/*
			 * January 1st, 2015	
			 */
			new String[]{MONTH_REPLACEMENT + "\\s*[0-9][a-zA-z]{2},\\s*[0-9]{4}", "MMMMM d'%s', yyyy"}, // this matches MMMM d'%s', yyyy
			new String[]{MONTH_REPLACEMENT + "\\s*[0-3][0-9][a-zA-z]{2},\\s*[0-9]{4}", "MMMMM dd'%s', yyyy"}, // this matches MMMM dd'%s', yyyy

			/*
			 * January 1, 2015	
			 */
			new String[]{MONTH_REPLACEMENT + "\\s*[0-9],\\s*[0-9]{4}", "MMMMM d, yyyy"},// this matches MMMM d, yyyy
			new String[]{MONTH_REPLACEMENT + "\\s*[0-3][0-9],\\s*[0-9]{4}", "MMMMM dd, yyyy"} // this matches MMMM dd, yyyy
		};
		
		numFormats = dateMatches.length;
		FIND_DATE : for(int i = 0; i < numFormats; i++) {
			String[] match = dateMatches[i];
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
	
	
	
	
	/**
	 * Determine date additional formatting
	 * @param type
	 * @param formatTracker
	 * @return
	 */
	public static Object[] determineDateFormatting(SemossDataType type, Map<String, Integer> formatTracker) {
		Object[] result = new Object[2];
		result[0] = type;
		if(formatTracker.size() == 1) {
			result[1] = formatTracker.keySet().iterator().next();
		} else {
			// trying to figure out the best match for the format
			// taking into consideration formats that are basically the same
			// but may contain 2 value (i.e. 11th day) vs 1 value (i.e. 1st day)
			// which matches to different patterns
			if(type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
				reconcileDateFormats(formatTracker);
			}
			
			// now just choose the most occuring one
			String mostOccuringFormat = Collections.max(formatTracker.entrySet(), Comparator.comparingInt(Map.Entry::getValue)).getKey();
			result[1] = mostOccuringFormat;
		}
		return result;
	}
	
	/**
	 * Try to reconcile different date formats
	 * @param formats
	 * @return
	 */
	private static void reconcileDateFormats(Map<String, Integer> formats) {
		int numFormats = formats.size();
		if(numFormats == 1) {
			return;
		}

		// loop and compare every format to every other format
		// once we have a match, we will recalculate
		String[] formatPaterns = formats.keySet().toArray(new String[numFormats]);
		char[] charsToFind = new char[]{'M', 'd', 'H', 'h', 'm', 's'};
		
		for(int i = 0; i < numFormats; i++) {
			String thisFormat = formatPaterns[i];
			// get the regex form of this
			String regexThisFormat = thisFormat;
			for(char c : charsToFind) {
				if(!regexThisFormat.contains(c + "")) {
					continue;
				}
				// trim the format first
				// so MM or dd becomes just M or d
				regexThisFormat = regexThisFormat.replaceAll(c + "{1,2}", c + "");
				int indexToFind = regexThisFormat.lastIndexOf(c);
				int len = regexThisFormat.length();
				regexThisFormat = regexThisFormat.substring(0, indexToFind+1) + "{1,2}" + regexThisFormat.substring(indexToFind+1, len);
			}
			
			Pattern p = Pattern.compile(regexThisFormat);
			for(int j = i+1; j < numFormats; j++) {
				String otherFormat = formatPaterns[j];

				Matcher matcher = p.matcher(otherFormat);
				if(matcher.find()) {
					// they are equivalent
					String largerFormat = thisFormat.length() > otherFormat.length() ? thisFormat : otherFormat;
					int c1 = formats.remove(thisFormat);
					int c2 = formats.remove(otherFormat);
					formats.put(largerFormat, c1+c2);
					// recursively go back and recalculate
					reconcileDateFormats(formats);
					return;
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
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
		
		d = "12/31";
		SimpleDateFormat sdf = new SimpleDateFormat("M/d");
		sdf.setLenient(false);
		System.out.println(sdf.parse(d));
	}

}
