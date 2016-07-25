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
