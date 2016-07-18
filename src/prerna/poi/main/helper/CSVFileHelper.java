package prerna.poi.main.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Vector;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import cern.colt.Arrays;
import prerna.poi.main.HeadersException;
import prerna.test.TestUtilityMethods;
import prerna.util.ArrayUtilityMethods;
import prerna.util.Utility;

public class CSVFileHelper {
	
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
    	settings.getFormat().setDelimiter(delimiter);
        settings.setEmptyValue("");
        settings.setSkipEmptyLines(true);
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
			newUniqueCSVHeaders = new Vector<String>(numCols);
			
			// create the integer array s.t. we can reset the value to get in the future
			headerIntegerArray = new Integer[numCols];
			// grab the headerChecker
			HeadersException headerChecker = HeadersException.getInstance();
			
			for(int colIdx = 0; colIdx < numCols; colIdx++) {
				String origHeader = allCsvHeaders[colIdx];
				String newHeader = recursivelyFixHeaders(origHeader, newUniqueCSVHeaders, headerChecker);
				
				// now update the unique headers, as this will be used to match duplications
				newUniqueCSVHeaders.add(newHeader);
				
				// fill in integer array
				headerIntegerArray[colIdx] = colIdx;
			}
		}
	}
	
	
	private String recursivelyFixHeaders(String origHeader, List<String> currCleanHeaders, HeadersException headerChecker) {
		boolean isAltered = false;
		
		/*
		 * For the following 3 checks
		 * Just perform a single fix within each block
		 * And let the recursion deal with having to fix an issue that is arising
		 * due to a previous fix
		 * i.e. you made a header no longer illegal but now it is a duplicate, recurssion of
		 * this method will deal with that
		 */
		
		// first, clean illegal characters
		if(headerChecker.containsIllegalCharacter(origHeader)) {
			origHeader = headerChecker.removeIllegalCharacters(origHeader);
			isAltered = true;
		}
		
		// second, check if header is some kind of reserved word
		if(headerChecker.isIllegalHeader(origHeader)) {
			origHeader = appendNumOntoHeader(origHeader);
			isAltered = true;
		}
		
		// third, check for duplications
		for(String currHead : currCleanHeaders) {
			if(origHeader.equalsIgnoreCase(currHead)) {
				origHeader = appendNumOntoHeader(origHeader);
				isAltered = true;
				break;
			}
		}
		
		// if we did alter the string at any point
		// we need to continue and re-run these checks again
		// until we have gone through without altering the string
		// and return the string
		if(isAltered) {
			origHeader = recursivelyFixHeaders(origHeader, currCleanHeaders, headerChecker);
		}
		
		return origHeader;
	}
	
	private String appendNumOntoHeader(String origHeader) {
		int num = 0;
		if(origHeader.matches(".*_\\d+")) {
			String strNumbers = origHeader.substring(origHeader.lastIndexOf("_"), origHeader.length());
			num = Integer.parseInt(strNumbers);
		}
		origHeader = origHeader  + "_" + (++num);
		
		return origHeader;
	}
	
	public String getHTMLBasedHeaderChanges() {
		StringBuilder htmlStr = new StringBuilder();
		htmlStr.append("Errors Found in Column Headers For File " + Utility.getOriginalFileName(this.fileLocation) + ". Performed the following changes to enable upload:<br>");
		htmlStr.append("<br>");
		htmlStr.append("COLUMN INDEX | OLD CSV NAME | NEW CSV NAME");

		boolean isChange = false;
		
		// loop through and find changes
		int numCols = allCsvHeaders.length;
		for(int colIdx = 0; colIdx < numCols; colIdx++) {
			String origHeader = allCsvHeaders[colIdx];
			String newHeader = newUniqueCSVHeaders.get(colIdx);
			
			if(!origHeader.equalsIgnoreCase(newHeader)) {
				isChange = true;
				htmlStr.append("<br>");
				htmlStr.append( (colIdx+1) + ") " + origHeader + " | " + newHeader);
			}
		}
		
		if(isChange) {
			return htmlStr.toString();
		} else {
			return null;
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
			getNextRow(); // to skip the header row
		}
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
		for(String col : newUniqueCSVHeaders) {
			parseColumns(new String[]{col});
			getNextRow();
			String type = null;
			String[] row = null;
			WHILE_LOOP : while( ( row = parser.parseNext()) != null) {
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
	

	///// TESTING CODE STARTS HERE /////
	
	public static void main(String [] args) throws Exception
	{
		// ugh, need to load this in for the header exceptions
		// this contains all the sql reserved words
		TestUtilityMethods.loadDIHelper();

		
		String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		long before, after;
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		fileName = "C:/Users/mahkhalil/Desktop/messedUpMovie.csv";
		before = System.nanoTime();
		CSVFileHelper test = new CSVFileHelper();
		test.parse(fileName);
		test.printRow(test.predictTypes());
		test.printRow(test.allCsvHeaders);
		System.out.println(test.newUniqueCSVHeaders);

		test.printRow(test.getNextRow());
		test.parseColumns(new String[]{"Title"});
		test.printRow(test.getNextRow());
		test.printRow(test.getNextRow());
		test.printRow(test.getNextRow());
		test.printRow(test.getNextRow());
		System.out.println(test.getHTMLBasedHeaderChanges());
		
		
//		test.allHeaders = null;
//		test.reset(false);
//		test.printRow(test.allHeaders);
//		System.out.println(test.countLines());
//		String [] columns = {"Title"};
//		test.parseColumns(columns);
//		test.printRow(test.getNextRow());
//		test.printRow(test.getNextRow());
//		test.printRow(test.getNextRow());
//		System.out.println(test.countLines());
//		test.reset(false);
//		//test.printRow(test.getRow());
//		after = System.nanoTime();
//		System.out.println((after - before)/1000000);
	}
	
	private int countLines() {
		int count = 0;
		while(getNextRow() != null) {
			count++;
		}
		return count;
	}

	private void printRow(String[] nextRow) {
		System.out.println(Arrays.toString(nextRow));
	}
	
//	public String[] cleanHeaders(String[] headers) {
//		String[] cleanHeaders = new String[headers.length];
//		for(int i = 0; i < headers.length; i++) {
//			cleanHeaders[i] = Utility.cleanVariableString(headers[i]);
//		}
//		return cleanHeaders;
//	}

}
