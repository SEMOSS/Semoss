package prerna.poi.main.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Map;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import cern.colt.Arrays;
import prerna.util.Utility;

public class CSVFileHelper {
	
	private CsvParser parser = null;
	private CsvParserSettings settings = null;
	private char delimiter = ',';
	
	private FileReader sourceFile = null;
	private String fileLocation = null;
	
	private String [] allHeaders = null;
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
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get the first row of the headers
	 */
	public void collectHeaders() {
		if(allHeaders == null) {
			allHeaders = getNextRow();
		}
	}
	
	/**
	 * Return the headers for the parser
	 * @return
	 */
	public String[] getHeaders() {
		if(this.currHeaders == null) {
			collectHeaders();
			return this.allHeaders;
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
		settings.selectFields(columns);
		currHeaders = columns;
		allHeaders = null;
		reset();
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
	public void reset() {
		clear();
		createParser();
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
	
	/**
	 * Get the file location
	 * @return		String with the file location
	 */
	public String getFileLocation() {
		return this.fileLocation;
	}
	
	/**
	 * Loop through all the data to see what the data types are for each column
	 * @return
	 */
	public Map<String, String> predictTypes() {
		Map<String, String> types = new Hashtable<String, String>();
		for(String col : allHeaders) {
			parseColumns(new String[]{col});
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
					if( (type.equals("BOOLEAN") || type.equals("INT") || type.equals("DOUBLE")) && 
							(newTypePred.equals("INT") || newTypePred.equals("INT") || newTypePred.equals("DOUBLE") ) ){
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
			types.put(col, type);
		}
		
		return types;
	}
	

	///// TESTING CODE STARTS HERE /////
	
	public static void main(String [] args) throws Exception
	{
		String fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		long before, after;
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/consumer_complaints.csv";
		//fileName = "C:/Users/pkapaleeswaran/workspacej3/datasets/Movie.csv";
		fileName = "C:/Users/mahkhalil/Desktop/Movie Results Fixed.csv";
		before = System.nanoTime();
		CSVFileHelper test = new CSVFileHelper();
		test.parse(fileName);
		System.out.println(test.predictTypes());
		test.printRow(test.getNextRow());
		test.printRow(test.allHeaders);
		test.allHeaders = null;
		test.reset();
		test.printRow(test.allHeaders);
		System.out.println(test.countLines());
		String [] columns = {"Title"};
		test.parseColumns(columns);
		test.printRow(test.getNextRow());
		test.printRow(test.getNextRow());
		test.printRow(test.getNextRow());
		System.out.println(test.countLines());
		test.reset();
		//test.printRow(test.getRow());
		after = System.nanoTime();
		System.out.println((after - before)/1000000);
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

}
