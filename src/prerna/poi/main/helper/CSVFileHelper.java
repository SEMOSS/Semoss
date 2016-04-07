package prerna.poi.main.helper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import cern.colt.Arrays;

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
	 * @param fileName		The String location of the fileName
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
