package prerna.date;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DatePatternGenerator {

	private DatePatternGenerator() {

	}

	/**
	 * Get simple date formats
	 * @param separator
	 * @return
	 */
	public static List<String[]> getBasicDateFormats(String separator) {
		List<String[]> retValues = new Vector<String[]>();

		String[][] month = getMonth();
		String[][] day = getDay();
		String[][] year = getYear();
		// month, day, year
		genSimpleCombinations(retValues, month, day, year, separator);
		// day, month, year
		genSimpleCombinations(retValues, day, month, year, separator);
		// year, month, day
		genSimpleCombinations(retValues, year, month, day, separator);
		// year, day, month
		genSimpleCombinations(retValues, year, day, month, separator);

		// month, year
		genSimpleCombinations(retValues, month, year, separator);
		// month, day
		genSimpleCombinations(retValues, month, day, separator);
		// day, month
		genSimpleCombinations(retValues, month, day, separator);
		
		return retValues;
	}
	
	/**
	 * Get date formats when date has MMM
	 * @return
	 */
	public static List<String[]> getPartialMonthDateFormats() {
		List<String[]> retValues = new Vector<String[]>();

		String[][] year = getYear();
		// month true, day false
		{
			String[][] month = getPartialMonth(true);
			String[][] day = getDay(false);

			// month, day, year
			genSimpleCombinations(retValues, month, day, year, "\\s*");
			// month, year
			genSimpleCombinations(retValues, month, year, "\\s*");
		}
		
		// month true, day true
		{
			String[][] month = getPartialMonth(true);
			String[][] day = getDay(true);
			// month, day
			genSimpleCombinations(retValues, month, day, "\\s*");
			// month, day
			genSimpleCombinations(retValues, month, day, "-");
		}
		
		// month false, day true
		{
			String[][] month = getPartialMonth(false);
			String[][] day = getDay(true);
			// day, month, year
			genSimpleCombinations(retValues, day, month, year, "\\s*");
			// year, month, day
			genSimpleCombinations(retValues, year, month, day, "\\s*");
			// day, month
			genSimpleCombinations(retValues, day, month, "\\s*");
			// day, month
			genSimpleCombinations(retValues, day, month, "-");
		}
		
		return retValues;
	}
	
	/**
	 * Get simple date formats
	 * @param separator
	 * @return
	 */
	public static List<String[]> getTimeFormats(String separator) {
		List<String[]> retValues = new Vector<String[]>();

		String[][] hr = getHour();
		String[][] min = getMinute();
		String[][] sec = getSecond();
		String[][] ms = getMilliSecond();
		// hr, min, sec
		genSimpleCombinations(retValues, hr, min, sec, separator);

		// hr, min, sec, ms
		genSimpleCombinations(retValues, hr, min, sec, ms, separator, "\\.");

		// hr, min
		genSimpleCombinations(retValues, hr, min, separator);
		// min, sec
		genSimpleCombinations(retValues, min, sec, separator);
		
		return retValues;
	}
	
	/**
	 * Get time stamp 
	 * @param dateSep
	 * @param timeSep
	 * @return
	 */
	public static List<String[]> getDateTimeFormats(String dateSep, String timeSep) {
		List<String[]> retValues = new Vector<String[]>();
		
		List<String[]> dateValues = getBasicDateFormats(dateSep);
		List<String[]> timeValues = getTimeFormats(timeSep);
		combineCombinations(retValues, dateValues, timeValues, "\\s*");
		
		return retValues;
	}
	
	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/*
	 * Date
	 */	
	
	private static String[][] getMonth() {
		return new String[][]{
			new String[]{"[1-9]", 			"M"},
			new String[]{"[1][0-2]", 		"M"},
			new String[]{"[0][1-9]", 		"MM"}
		};
	}
	
	private static String[][] getPartialMonth(boolean basic) {
		if(basic) {
			return new String[][]{
				new String[]{"[a-zA-Z]{3}", 	"MMM"},
			};
		} else {
			return new String[][]{
				new String[]{"[a-zA-Z]{3}",		"MMM"},
				new String[]{"[a-zA-Z]{3},", 	"MMM,"},
			};
		}
	}

	private static String[][] getDay() {
		return getDay(true);
	}

	private static String[][] getDay(boolean basic) {
		if(basic) {
			return new String[][]{
				new String[]{"[1-9]", 		"d"},
				new String[]{"[1-3][0-9]", 	"d"},
				new String[]{"[0][1-9]", 	"dd"}
			};
		} else {
			return new String[][]{
				// without comma
				new String[]{"[1-9]", 		"d"},
				new String[]{"[1-3][0-9]", 	"d"},
				new String[]{"[0][1-9]", 	"dd"},
				// with comma
				new String[]{"[1-9],", 		"d,"},
				new String[]{"[1-3][0-9],", "d,"},
				new String[]{"[0][1-9],", 	"dd,"}
			};
		}
	}

	private static String[][] getYear() {
		return new String[][]{
			new String[]{"[0-9]{4}", "yyyy"},
			new String[]{"[0-9]{2}", "yy"}
		};
	}
	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	
	/*
	* Time
	*/
	
	private static String[][] getHour() {
		return new String[][]{
			new String[]{"[0][0-9]", 		"HH"},
			new String[]{"[0-9]", 			"H"},
			new String[]{"[1][0-9]", 		"H"},
			new String[]{"[2][0-3]", 		"H"}
		};
	}
	
	private static String[][] getMinute() {
		return new String[][]{
			new String[]{"[0][0-9]", 		"mm"},
			new String[]{"[1-5][0-9]", 		"m"},
			new String[]{"[6][0]", 			"m"}
		};
	}
	
	private static String[][] getSecond() {
		return new String[][]{
			new String[]{"[0][0-9]", 		"ss"},
			new String[]{"[1-5][0-9]", 		"s"},
			new String[]{"[6][0]", 			"s"}
		};
	}
	
	private static String[][] getMilliSecond() {
		return new String[][]{
			new String[]{"[0-9]{4}", 				"S"},
			new String[]{"[0][0-9]{3}", 			"SS"},
			new String[]{"[0][0][0-9]{2}", 			"SSS"},
			new String[]{"[0][0][0][0-9]{1}", 		"SSSS"}
		};
	}
	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////

	/*
	 * Combinations
	 */
	
	/**
	 * Loop through and combine all the possibilities
	 * @param retValues
	 * @param comb1
	 * @param comb2
	 * @param separator
	 */
	private static void genSimpleCombinations(List<String[]> retValues, String[][] comb1, String[][] comb2, String separator) {
		String patternSep = separator;
		if(patternSep.equals("\\s*")) {
			patternSep = " ";
		}
		int size1 = comb1.length;
		int size2 = comb2.length;
		for(int i = 0; i < size1; i++) {
			String[] val1 = comb1[i];
			for(int j = 0; j < size2; j++) {
				String[] val2 = comb2[j];
				StringBuilder regexMatch = new StringBuilder();
				regexMatch.append(val1[0]).append(separator).append(val2[0]);

				StringBuilder pattern = new StringBuilder();
				pattern.append(val1[1]).append(patternSep).append(val2[1]);

				// add value
				retValues.add(new String[]{regexMatch.toString(), pattern.toString()});
			}
		}
	}

	/**
	 * Loop through and combine all the possibilities
	 * @param retValues
	 * @param comb1
	 * @param comb2
	 * @param comb3
	 * @param separator
	 */
	private static void genSimpleCombinations(List<String[]> retValues, String[][] comb1, String[][] comb2, String[][] comb3, String separator) {
		String patternSep = separator;
		if(patternSep.equals("\\s*")) {
			patternSep = " ";
		}
		int size1 = comb1.length;
		int size2 = comb2.length;
		int size3 = comb3.length;
		for(int i = 0; i < size1; i++) {
			String[] val1 = comb1[i];
			for(int j = 0; j < size2; j++) {
				String[] val2 = comb2[j];
				for(int k = 0; k < size3; k++) {
					String[] val3 = comb3[k];
					StringBuilder regexMatch = new StringBuilder();
					regexMatch.append(val1[0]).append(separator).append(val2[0]).append(separator).append(val3[0]);

					StringBuilder pattern = new StringBuilder();
					pattern.append(val1[1]).append(patternSep).append(val2[1]).append(patternSep).append(val3[1]);

					// add value
					retValues.add(new String[]{regexMatch.toString(), pattern.toString()});
				}
			}
		}
	}
	
	/**
	 * Loop through and combine all the possibilities
	 * @param retValues
	 * @param comb1
	 * @param comb2
	 * @param comb3
	 * @param separator
	 */
	private static void genSimpleCombinations(List<String[]> retValues, 
			String[][] comb1, 
			String[][] comb2, 
			String[][] comb3, 
			String[][] comb4, 
			String separator, 
			String finalSeparator) {
		String patternSep = separator;
		String finalPSep = finalSeparator;
		if(patternSep.equals("\\s*")) {
			patternSep = " ";
		}
		if(finalSeparator.equals("\\.")) {
			finalPSep = ".";
		}
		int size1 = comb1.length;
		int size2 = comb2.length;
		int size3 = comb3.length;
		int size4 = comb4.length;
		
		for(int i = 0; i < size1; i++) {
			String[] val1 = comb1[i];
			for(int j = 0; j < size2; j++) {
				String[] val2 = comb2[j];
				for(int k = 0; k < size3; k++) {
					String[] val3 = comb3[k];
					for(int l = 0; l < size4; l++) {
						String[] val4 = comb4[l];
						StringBuilder regexMatch = new StringBuilder();
						regexMatch.append(val1[0]).append(separator).append(val2[0]).append(separator).append(val3[0]).append(finalPSep).append(val4[0]);

						StringBuilder pattern = new StringBuilder();
						pattern.append(val1[1]).append(patternSep).append(val2[1]).append(patternSep).append(val3[1]).append(finalPSep).append(val4[1]);

						// add value
						retValues.add(new String[]{regexMatch.toString(), pattern.toString()});
					}
				}
			}
		}
	}
	
	/**
	 * Combine 2 sets of possiblities
	 * Used when combining all date possibilities with time possibilities
	 * @param retValues
	 * @param comb1
	 * @param comb2
	 * @param combinationSep
	 */
	private static void combineCombinations(List<String[]> retValues, List<String[]> comb1, List<String[]> comb2, String combinationSep) {
		String patternSep = combinationSep;
		if(patternSep.equals("\\s*")) {
			patternSep = " ";
		}
		
		int size1 = comb1.size();
		int size2 = comb2.size();
		
		for(int i = 0; i < size1; i++) {
			String[] val1 = comb1.get(i);
			for(int j = 0; j < size2; j++) {
				String[] val2 = comb2.get(j);
				
				StringBuilder regexMatch = new StringBuilder();
				regexMatch.append(val1[0]).append(combinationSep).append(val2[0]);

				StringBuilder pattern = new StringBuilder();
				pattern.append(val1[1]).append(patternSep).append(val2[1]);

				// add combined value
				retValues.add(new String[]{regexMatch.toString(), pattern.toString()});
			}
		}
	}

	
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////////////////////
	
	/*
	* Testing
	*/


//	public static void main(String[] args) {
//		System.out.println("DATE FORMATS!!!!");
//		System.out.println("DATE FORMATS!!!!");
//		System.out.println("DATE FORMATS!!!!");
//		{
//			List<String[]> dateValues = DatePatternGenerator.getBasicDateFormats("/");
//			for(String[] vals : dateValues) {
//				System.out.println(Arrays.toString(vals));
//			}
//		}
//		System.out.println("PARTIAL DATE FORMATS!!!!");
//		System.out.println("PARTIAL DATE FORMATS!!!!");
//		System.out.println("PARTIAL DATE FORMATS!!!!");
//		{
//			List<String[]> dateValues = DatePatternGenerator.getPartialMonthDateFormats();
//			for(String[] vals : dateValues) {
//				System.out.println(Arrays.toString(vals));
//			}
//		}
//		System.out.println("TIME STAMP FORMATS!!!!");
//		System.out.println("TIME STAMP FORMATS!!!!");
//		System.out.println("TIME STAMP FORMATS!!!!");
//		{
//			List<String[]> dateValues = DatePatternGenerator.getDateTimeFormats("/", ":");
//			for(String[] vals : dateValues) {
//				System.out.println(Arrays.toString(vals));
//			}
//		}
//	}

	
	
	
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
}
