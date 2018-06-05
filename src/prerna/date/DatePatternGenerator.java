package prerna.date;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

public class DatePatternGenerator {

	private DatePatternGenerator() {

	}

	public static List<String[]> getBasicDateFormats(String separator) {
		List<String[]> retValues = new Vector<String[]>();

		String[][] month = getMonth();
		String[][] day = getDay();
		String[][] year = getYear();
		// month, day, year
		generateCombinations(retValues, month, day, year, separator);
		// day, month, year
		generateCombinations(retValues, day, month, year, separator);
		// year, month, day
		generateCombinations(retValues, year, month, day, separator);

		// month, year
		generateCombinations(retValues, month, year, separator);
		// month, day
		generateCombinations(retValues, month, day, separator);
		// day, month
		generateCombinations(retValues, month, day, separator);
		
		return retValues;
	}
	
	public static List<String[]> getComplexMonth() {
		List<String[]> retValues = new Vector<String[]>();

		String[][] year = getYear();
		// month true, day false
		{
			String[][] month = getPartialMonth(true);
			String[][] day = getDay(false);

			// month, day, year
			generateCombinations(retValues, month, day, year, "\\s*");
			// month, year
			generateCombinations(retValues, month, year, "\\s*");
		}
		
		// month true, day true
		{
			String[][] month = getPartialMonth(true);
			String[][] day = getDay(true);
			// month, day
			generateCombinations(retValues, month, day, "\\s*");
			// month, day
			generateCombinations(retValues, month, day, "-");
		}
		
		// month false, day true
		{
			String[][] month = getPartialMonth(false);
			String[][] day = getDay(true);
			// day, month, year
			generateCombinations(retValues, day, month, year, "\\s*");
			// year, month, day
			generateCombinations(retValues, year, month, day, "\\s*");
			// day, month
			generateCombinations(retValues, month, day, "\\s*");
			// day, month
			generateCombinations(retValues, month, day, "-");
		}
		
//		/*
//		 * Wed, Mar 12 2015
//		 */
//		new String[]{"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-9]\\s*[0-9]{4}", "EEE, MMM d yyyy"}, // this matches EEE, MMM d yyyy
//		new String[]{"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-3][0-9]\\s*[0-9]{4}", "EEE, MMM dd yyyy"}, // this matches EEE, MMM dd yyyy
//
//		// additional comma compared to above
//		new String[]{"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-9],\\s*[0-9]{4}", "EEE, MMM d, yyyy"}, // this matches EEE, MMM d, yyyy
//		new String[]{"[a-zA-Z]{3},\\s*[a-zA-Z]{3}\\s*[0-3][0-9],\\s*[0-9]{4}", "EEE, MMM dd, yyyy"}, // this matches EEE, MMM dd, yyyy
//
//		/*
//		 * Wed, 12 Mar 2015
//		 */
//		new String[]{"[a-zA-Z]{3},\\s*[0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", "EEE, d MMM yyyy"}, // this matches EEE, d MMM yyyy
//		new String[]{"[a-zA-Z]{3},\\s*[0-3][0-9]\\s*[a-zA-Z]{3}\\s*[0-9]{4}", "EEE, dd MMM yyyy"}, // this matches EEE, dd MMM yyyy
//
//		// additional comma compared to above
//		new String[]{"[a-zA-Z]{3},\\s*[0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", "EEE, d MMM, yyyy"}, // this matches EEE, d MMM, yyyy
//		new String[]{"[a-zA-Z]{3},\\s*[0-3][0-9]\\s*[a-zA-Z]{3},\\s*[0-9]{4}", "EEE, dd MMM, yyyy"}// this matches EEE, dd MMM, yyyy
		
		return retValues;
	}
	
	private static String[][] getPartialMonth(boolean basic) {
		if(basic) {
			return new String[][]{
				new String[]{"[a-zA-Z]{3}", "MMM"},
			};
		} else {
			return new String[][]{
				new String[]{"[a-zA-Z]{3}", " MMM"},
				new String[]{"[a-zA-Z]{3},", " MMM,"},
			};
		}
	}

	private static String[][] getMonth() {
		return new String[][]{
			new String[]{"[1-9]", "M"},
			new String[]{"[1][0-9]", "M"},
			new String[]{"[0][1-9]", "MM"}
		};
	}

	private static String[][] getDay() {
		return getDay(true);
	}
	
	private static String[][] getDay(boolean basic) {
		if(basic) {
			return new String[][]{
				new String[]{"[1-9]", "d"},
				new String[]{"[1-3][0-9]", "d"},
				new String[]{"[0][1-9]", "dd"}
			};
		} else {
			return new String[][]{
				// without comma
				new String[]{"[1-9]", "d"},
				new String[]{"[1-3][0-9]", "d"},
				new String[]{"[0][1-9]", "dd"},
				// with comma
				new String[]{"[1-9],", "d,"},
				new String[]{"[1-3][0-9],", "d,"},
				new String[]{"[0][1-9],", "dd,"}
			};
		}
	}

	private static String[][] getYear() {
		return new String[][]{
			new String[]{"[0-9]{4}", "yyyy"},
			new String[]{"[0-9]{2}", "yy"}
		};
	}

	/**
	 * Loop through and combine all the possibilities
	 * @param retValues
	 * @param comb1
	 * @param comb2
	 * @param separator
	 */
	private static void generateCombinations(List<String[]> retValues, String[][] comb1, String[][] comb2, String separator) {
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
	private static void generateCombinations(List<String[]> retValues, String[][] comb1, String[][] comb2, String[][] comb3, String separator) {
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



	public static void main(String[] args) {
//		{
//			List<String[]> dateValues = DatePatternGenerator.getBasicDateFormats("/");
//			for(String[] vals : dateValues) {
//				System.out.println(Arrays.toString(vals));
//			}
//		}
		{
			List<String[]> dateValues = DatePatternGenerator.getComplexMonth();
			for(String[] vals : dateValues) {
				System.out.println(Arrays.toString(vals));
			}
		}
	}

}
