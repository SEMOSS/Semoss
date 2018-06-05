package prerna.date;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
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
			return genBasicDateObj(input, DatePatternGenerator.getBasicDateFormats("/"));
		} else if(!containsAlpha && input.contains("-")) {
			return genBasicDateObj(input, DatePatternGenerator.getBasicDateFormats("-"));
		} else {
			// this is checking that it doesn't only contain numbers and / and -
			return genBasicDateObj(input, DatePatternGenerator.getComplexMonth());
		}
	}
	
	/**
	 * Try to match with inputs that contain a /
	 * @param input
	 * @return
	 */
	private static SemossDate genBasicDateObj(String input, List<String[]> dateMatches) {
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
		
		d = "12/31";
		SimpleDateFormat sdf = new SimpleDateFormat("M/d");
		sdf.setLenient(false);
		System.out.println(sdf.parse(d));
	}

}
