package prerna.poi.main.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import cern.colt.Arrays;
import prerna.algorithm.api.SemossDataType;
import prerna.poi.main.HeadersException;
import prerna.test.TestUtilityMethods;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class CSVFileHelper {
	
	private static final int NUM_ROWS_TO_PREDICT_TYPES = 1000;
	
	private CsvParser parser = null;
	private CsvParserSettings settings = null;
	private char delimiter = ',';

	private FileReader sourceFile = null;
	private String fileLocation = null;

	// we need to keep two sets of headers
	// we will keep the headers as is within the physical file
	private String [] allCsvHeaders = null;
	// ... that is all good and all, but when we have duplicates, it 
	// messes things up. to reduce complexity elsewhere, we will just 
	// create a new unique csv headers string[] to store the values
	// this will in essence become the new "physical names" for each
	// column
	private List<String> newUniqueCSVHeaders = null;

	// keep track of integer with values s.t. we can easily reset to get all the values
	// without getting an error when there are duplicate headers within the univocity api
	// this will literally be [0,1,2,3,...,n] where n = number of columns - 1
	private Integer [] headerIntegerArray = null;


	// keep track of the current headers being used
	private String [] currHeaders = null;

	/*
	 * THIS IS REALLY ANNOYING
	 * In thick client, need to know if the last column is 
	 * the path to the prop file location for csv upload
	 */
	private boolean propFileExists = false;

	// api stores max values for security reasons
	private int maxColumns = 1_000_000;
	private int maxCharsPerColumn = 1_000_000;
	
	/**
	 * Parse the new file passed
	 * @param fileLocation		The String location of the fileName
	 */
	public void parse(String fileLocation) {
		this.fileLocation = fileLocation;
		makeSettings();
		createParser();
	}

	/**
	 * Generate a new settings object to parse based on a set delimiter
	 */
	private void makeSettings() {
		settings = new CsvParserSettings();
		settings.setNullValue("");
		CsvFormat parseFormat = settings.getFormat();
		parseFormat.setDelimiter(delimiter);
		parseFormat.setLineSeparator(NewLinePredictor.predict(this.fileLocation));
		settings.setEmptyValue("");
		settings.setSkipEmptyLines(true);
		// override default values
		settings.setMaxColumns(maxColumns);
		settings.setMaxCharsPerColumn(maxCharsPerColumn);
		// get the headers ?
		//settings.setHeaderExtractionEnabled(true);
	}

	/**
	 * Creates the parser 
	 */
	private void createParser() {
		parser = new CsvParser(settings);
		try {
			File file = new File(fileLocation);
			sourceFile = new FileReader(file);
			parser.beginParsing(sourceFile);
			collectHeaders();

			// since files can be dumb and contain multiple indices
			// we need to keep a map of the header to the index

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Get the first row of the headers
	 */
	public void collectHeaders() {
		if(allCsvHeaders == null) {
			allCsvHeaders = getNextRow();

			// need to keep track and make sure our headers are good
			int numCols = allCsvHeaders.length;
			/*
			 * THIS IS REALLY ANNOYING
			 * In thick client, need to know if the last column is 
			 * the path to the prop file location for csv upload
			 */
			if(propFileExists) {
				numCols--;
			}
			newUniqueCSVHeaders = new Vector<String>(numCols);

			// create the integer array s.t. we can reset the value to get in the future
			headerIntegerArray = new Integer[numCols];
			// grab the headerChecker
			HeadersException headerChecker = HeadersException.getInstance();

			for(int colIdx = 0; colIdx < numCols; colIdx++) {
				// just trim all the headers
				allCsvHeaders[colIdx] = allCsvHeaders[colIdx].trim();
				String origHeader = allCsvHeaders[colIdx];
				if(origHeader.trim().isEmpty()) {
					origHeader = "BLANK_HEADER";
				}
				String newHeader = headerChecker.recursivelyFixHeaders(origHeader, newUniqueCSVHeaders);

				// now update the unique headers, as this will be used to match duplications
				newUniqueCSVHeaders.add(newHeader);

				// fill in integer array
				headerIntegerArray[colIdx] = colIdx;
			}
		}
	}

	/**
	 * Return the headers for the parser
	 * @return
	 */
	public String[] getHeaders() {
		if(this.currHeaders == null) {
			collectHeaders();
			return this.newUniqueCSVHeaders.toArray(new String[]{});
		}
		return this.currHeaders;
	}
	
	public void setUsingPropFile(boolean propFileExist) {
		this.propFileExists  = propFileExist;
	}

	/**
	 * Get all the headers used in the csv file
	 * This is the clean version of the csv headers
	 * @return
	 */
	public String[] getAllCSVHeaders() {
		return this.newUniqueCSVHeaders.toArray(new String[]{});
	}


	/**
	 * Set a limit on which columns you want to be parsed
	 * @param columns			The String[] containing the headers you want
	 */
	public void parseColumns(String[] columns) {
		// map it back to clean columns
		makeSettings();

		// must use index for when there are duplicate values
		Integer[] values = new Integer[columns.length];
		for(int colIdx = 0; colIdx < columns.length; colIdx++) {
			values[colIdx] = newUniqueCSVHeaders.indexOf(columns[colIdx]);
		}
		settings.selectIndexes(values);
		currHeaders = columns;
		reset(false);
	}

	/**
	 * Get the next row of the file
	 * @return
	 */
	public String[] getNextRow() {
		return parser.parseNext();
	}

	/**
	 * Reset to start the parser from the beginning of the file
	 */
	public void reset(boolean removeCurrHeaders) {
		clear();
		createParser();
		if(removeCurrHeaders) {
			currHeaders = null;
			// setting the indices to be all the headers
			settings.selectIndexes(headerIntegerArray);
		}
		// this is to get the header row
		getNextRow();
	}

	/**
	 * Clears the parser and requires you to start the parsing from scratch	
	 */
	public void clear() {
		try {
			if(sourceFile != null) {
				sourceFile.close(); 
			}
			if(parser != null) {
				parser.stopParsing();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Set the delimiter for parser
	 * @param charAt
	 */
	public void setDelimiter(char charAt) {
		this.delimiter = charAt;
	}

	public char getDelimiter() {
		return this.delimiter;
	}

	/**
	 * Get the file location
	 * @return		String with the file location
	 */
	public String getFileLocation() {
		return this.fileLocation;
	}

	public String[] orderHeadersToGet(String[] headersToGet) {
		String[] orderedHeaders = new String[headersToGet.length];
		int counter = 0;
		for(String header : this.newUniqueCSVHeaders) {
			if(ArrayUtilityMethods.arrayContainsValue(headersToGet, header)) {
				orderedHeaders[counter] = header;
				counter++;
			}
		}
		return orderedHeaders;
	}


	/**
	 * Loop through all the data to see what the data types are for each column
	 * @return
	 */
	public String[] predictTypes() {
		String[] types = new String[newUniqueCSVHeaders.size()];
		int counter = 0;
		// Loop through cols, and up to 1000 rows
		for(String col : newUniqueCSVHeaders) {
			int rowCounter = 0;
			parseColumns(new String[]{col});
			//			getNextRow();
			String type = null;
			String[] row = null;
			WHILE_LOOP : while(rowCounter < NUM_ROWS_TO_PREDICT_TYPES && ( row = parser.parseNext()) != null) {
				String val = row[0];
				if(val.isEmpty()) {
					continue;
				}
				String newTypePred = (Utility.findTypes(val)[0] + "").toUpperCase();
				if(newTypePred.contains("VARCHAR")) {
					type = newTypePred;
					break WHILE_LOOP;
				}

				// need to also add the type null check for the first row
				if(!newTypePred.equals(type) && type != null) {
					// this means there are multiple types in one column
					// assume it is a string 
					if( (type.equals("INT") || type.equals("DOUBLE")) && (newTypePred.equals("INT") || 
							newTypePred.equals("INT") || newTypePred.equals("DOUBLE") ) ){
						// for simplicity, make it a double and call it a day
						// TODO: see if we want to impl the logic to choose the greater of the newest
						// this would require more checks though
						type = "DOUBLE";
					} else {
						// should only enter here when there are numbers and dates
						// TODO: need to figure out what to handle this case
						// for now, making assumption to put it as a string
						type = "VARCHAR(800)";
						break WHILE_LOOP;
					}
				} else {
					// type is the same as the new predicated type
					// or type is null on first iteration
					type = newTypePred;
				}
				
				rowCounter++;
			}
			// if an entire column is empty, type will be null
			// why someone has a csv file with an empty column, i do not know...
			if(type == null) {
				type = "VARCHAR(800)";
			}
			types[counter] = type;
			counter++;
		}

		reset(true);
		return types;
	}
	
	/**
	 * Loop through all the data to see what the data types are for each column
	 * @return
	 */
	public Object[][] predictTypes2() {
		int numCols = newUniqueCSVHeaders.size();
		Object[][] predictedTypes = new Object[numCols][3];
		List<Map<String, Integer>> additionalFormatTracker = new Vector<Map<String, Integer>>(numCols);
		// loop through cols, and up to 1000 rows
		for(int counter = 0; counter < numCols; counter++) {
			// grab the column
			String col = newUniqueCSVHeaders.get(counter);
			parseColumns(new String[]{col});

			int rowCounter = 0;
			SemossDataType type = null;
			Map<String, Integer> formatTracker = new HashMap<String, Integer>();
			additionalFormatTracker.add(counter, formatTracker);
			
			String[] row = null;
			WHILE_LOOP : while(rowCounter < NUM_ROWS_TO_PREDICT_TYPES && ( row = parser.parseNext()) != null) {
				String val = row[0];
				if(val.isEmpty()) {
					continue;
				}
				Object[] prediction = Utility.determineInputType(val);
				SemossDataType newTypePrediction = (SemossDataType) prediction[1];
				
				// handle the additional formatting
				if(prediction[2] != null) {
					String fValue = prediction[2] + "";
					if(formatTracker.containsKey(fValue)) {
						// increase counter by 1
						formatTracker.put(fValue, new Integer(formatTracker.get(fValue) + 1));
					} else {
						formatTracker.put(fValue, new Integer(1));
					}
				}
				
				// if we hit a string
				// we are done
				if(newTypePrediction == SemossDataType.STRING || newTypePrediction == SemossDataType.BOOLEAN) {
					Object[] columnPrediction = new Object[2];
					columnPrediction[0] = SemossDataType.STRING;
					predictedTypes[counter] = columnPrediction;
					break WHILE_LOOP;
				}
				
				if(type == null) {
					// this is the first time we go through
					// just set the type and we are done
					// we only need to go through when we hit a difference
					type = newTypePrediction;
					continue;
				}
				
				if(type == newTypePrediction) {
					// well, nothing for us to do if its the same
					// again, we handle additional formatting
					// at the top
					continue;
				}
				
				// if we hit an integer
				else if(newTypePrediction == SemossDataType.INT) {
					if(type == SemossDataType.DOUBLE) {
						// the type stays as double
						type = SemossDataType.DOUBLE;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break WHILE_LOOP;
					}
				}
				
				// if we hit a double
				else if(newTypePrediction == SemossDataType.DOUBLE) {
					if(type == SemossDataType.INT) {
						// the type stays as double
						type = SemossDataType.DOUBLE;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break WHILE_LOOP;
					}
				}
				
				// if we hit a date
				else if(newTypePrediction == SemossDataType.DATE) {
					if(type == SemossDataType.TIMESTAMP) {
						// stick with timestamp
						type = SemossDataType.TIMESTAMP;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break WHILE_LOOP;
					}
				}
				
				// if we hit a timestamp
				else if(newTypePrediction == SemossDataType.TIMESTAMP) {
					if(type == SemossDataType.DATE) {
						// stick with timestamp
						type = SemossDataType.TIMESTAMP;
					} else {
						// we have a number and something else we dont know
						// default to string
						type = SemossDataType.STRING;
						// clear the tracker so we dont send additional format logic
						formatTracker.clear();
						break WHILE_LOOP;
					}
				}
				
				rowCounter++;
			}
			// if an entire column is empty, type will be null
			// why someone has a csv file with an empty column, i do not know...
			if(type == null) {
				type = SemossDataType.STRING;
			}
			
			// if format tracking is empty
			// just add the type to the matrix
			// and continue
			if(formatTracker.isEmpty()) {
				Object[] columnPrediction = new Object[2];
				columnPrediction[0] = type;
				predictedTypes[counter] = columnPrediction;
			} else {
				// format tracker is not empty
				// need to figure out the date situation
				if(type == SemossDataType.DATE || type == SemossDataType.TIMESTAMP) {
					Object[] results = determineDateFormatting(type, formatTracker);
					predictedTypes[counter] = results;
				} else {
					// UGH... how did you get here if you are not a date???
					Object[] columnPrediction = new Object[2];
					columnPrediction[0] = type;
					predictedTypes[counter] = columnPrediction;
				}
			}
		}

		reset(true);
		return predictedTypes;
	}

	/**
	 * Determine date additional formatting
	 * @param type
	 * @param formatTracker
	 * @return
	 */
	private Object[] determineDateFormatting(SemossDataType type, Map<String, Integer> formatTracker) {
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
	public void reconcileDateFormats(Map<String, Integer> formats) {
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
	
	public Map<String, String> getChangedHeaders() {
		Map<String, String> modHeaders = new Hashtable<String, String>();
		// loop through and find changes
		int numCols = allCsvHeaders.length;
		for(int colIdx = 0; colIdx < numCols; colIdx++) {
			String origHeader = allCsvHeaders[colIdx];
			String newHeader = newUniqueCSVHeaders.get(colIdx);

			if(!origHeader.equalsIgnoreCase(newHeader)) {
				modHeaders.put(newHeader, "Original Header Value = " + origHeader);
			}
		}

		return modHeaders;
	}
	
	public void modifyCleanedHeaders(Map<String, String> thisFileHeaderChanges) {
		// iterate through all sets of oldHeader -> newHeader
		for(String oldHeader : thisFileHeaderChanges.keySet()) {
			String desiredNewHeaderValue = thisFileHeaderChanges.get(oldHeader);
			
			// since the user may not want all the headers, we only check if new headers are valid
			// based on the headers they want
			// thus, we need to check and see if the newHeaderValue is actually already used
			int newNameIndex = this.newUniqueCSVHeaders.indexOf(desiredNewHeaderValue);
			if(newNameIndex >= 0) {
				// this new header exists
				// lets modify it
				this.newUniqueCSVHeaders.set(newNameIndex, "NOT_USED_COLUMN_1234567890");
			}
			
			// now we modify what was the old header to be the new header
			int oldHeaderIndex = this.newUniqueCSVHeaders.indexOf(oldHeader);
			this.newUniqueCSVHeaders.set(oldHeaderIndex, desiredNewHeaderValue);
		}
	}

	/**
	 * Get each csv column name as it exists in the csv file to the valid csv header 
	 * that we create on the BE
	 * If the name is already valid, it just points to itself
	 * @return
	 */
	public List<String[]> getExistingToModifedHeaders() {
		List<String[]> modHeaders = new Vector<String[]>();
		// loop through and find changes
		int numCols = allCsvHeaders.length;
		for(int colIdx = 0; colIdx < numCols; colIdx++) {
			String[] modified = new String[]{allCsvHeaders[colIdx], newUniqueCSVHeaders.get(colIdx)};
			modHeaders.add(modified);
		}

		return modHeaders;
	}
	
	/**
	 * This will return the original headers of the csv file
	 * Before we performed any cleaning of data
	 * @return
	 */
	public String[] getFileOriginalHeaders() {
		return this.allCsvHeaders;
	}

	///// TESTING CODE STARTS HERE /////

	public static void main(String [] args) throws Exception {
		// ugh, need to load this in for the header exceptions
		// this contains all the sql reserved words
		TestUtilityMethods.loadDIHelper();

		String fileName = "C:/Users/SEMOSS/Desktop/data.csv";
		CSVFileHelper test = new CSVFileHelper();
		
		test.parse(fileName);
		test.getHeaders();
		Object[][] predictions = test.predictTypes2();
		for(Object[] r : predictions) {
			System.out.println(Arrays.toString(r));
		}
		
	}
}
