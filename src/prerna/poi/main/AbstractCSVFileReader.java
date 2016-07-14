package prerna.poi.main;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.main.helper.CSVFileHelper;

public abstract class AbstractCSVFileReader extends AbstractFileReader {

	private static final Logger LOGGER = LogManager.getLogger(RDBMSReader.class.getName());

	// stores a list of rdf maps created from the FE
	protected Hashtable<String, String>[] rdfMapArr;
	protected List<String> relationArrayList = new ArrayList<String>();
	protected List<String> nodePropArrayList = new ArrayList<String>();
	protected List<String> relPropArrayList = new ArrayList<String>();
	protected boolean propFileExist = true;
	
	// fields around the csv file
	protected CSVFileHelper csvHelper;
	protected String [] header;
	protected String[] dataTypes;
	protected Map<String, Integer> csvColumnToIndex;
	
	protected int count = 0;
	protected int startRow = 2;
	protected int maxRows = 10000000;

	// keep conversion from user input to sql datatypes
	protected Map<String, String> sqlHash = new Hashtable<String, String>();

	/**
	 * Specifies which rows in the CSV to load based on user input in the prop file
	 * @throws FileReaderException 
	 */
	public void skipRows() throws IOException {
		//start count at 1 just row 1 is the header
		count = 1;
		if (rdfMap.get("START_ROW") != null) {
			startRow = Integer.parseInt(rdfMap.get("START_ROW")); 
		}
		while( count<startRow-1 && csvHelper.getNextRow() != null)// && count<maxRows)
		{
			count++;
		}
	}
	
	/**
	 * Load the CSV file
	 * Gets the headers for each column and reads the property file
	 * @param fileName String
	 * @throws FileNotFoundException 
	 */
	protected void openCSVFile(final String FILE_LOCATION) {
		LOGGER.info("Processing csv file: " + FILE_LOCATION);

		// use the csv file helper to load the data
		csvHelper = new CSVFileHelper();
		// assume csv
		csvHelper.setDelimiter(',');
		csvHelper.parse(FILE_LOCATION);

		// get the headers for the csv
		this.header = csvHelper.getHeaders();
		LOGGER.info("Found headers: " + Arrays.toString(header));
	}

	/**
	 * Fill in the sqlHash with the types
	 */
	protected void createSQLTypes() {
		sqlHash.put("DECIMAL", "FLOAT");
		sqlHash.put("DOUBLE", "FLOAT");
		sqlHash.put("STRING", "VARCHAR(2000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
		sqlHash.put("TEXT", "VARCHAR(2000)"); // 8000 was chosen because this is the max for SQL Server; needs more permanent fix
		//TODO: the FE needs to differentiate between "dates with times" vs. "dates"
		sqlHash.put("DATE", "DATE");
		sqlHash.put("SIMPLEDATE", "DATE");
		// currently only add in numbers as doubles
		sqlHash.put("NUMBER", "FLOAT");
		sqlHash.put("INTEGER", "FLOAT");
		sqlHash.put("BOOLEAN", "BOOLEAN");
	}

	/**
	 * Closes the CSV file streams
	 * @throws IOException 
	 */
	public void closeCSVFile() {
		if(csvHelper != null) {
			csvHelper.clear();
		}
	}

	/**
	 * Setter to store the metamodel created by user as a Hashtable
	 * @param data	Hashtable<String, String> containing all the information in a properties file
	 */
	public void setRdfMapArr(Hashtable<String, String>[] rdfMapArr) {
		this.rdfMapArr = rdfMapArr;
		propFileExist = false;
	}
	
	
}
