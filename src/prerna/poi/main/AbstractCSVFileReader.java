//package prerna.poi.main;
//
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import prerna.poi.main.helper.CSVFileHelper;
//
//public abstract class AbstractCSVFileReader extends AbstractFileReader {
//
//	private static final Logger logger = LogManager.getLogger(RDBMSReader.class.getName());
//
//	protected List<String> relationArrayList = new ArrayList<String>();
//	protected List<String> nodePropArrayList = new ArrayList<String>();
//	protected List<String> relPropArrayList = new ArrayList<String>();
//	
//	// fields around the csv file
//	protected CSVFileHelper csvHelper;
//	protected String [] header;
//	protected String[] dataTypes;
//	protected Map<String, Integer> csvColumnToIndex;
//	
//	/*
//	 * Store the new user defined csv file names
//	 * Format for this is:
//	 * {
//	 * 		csv_file_name1 -> {
//	 * 							fixed_header_name_1 -> user_changed_header_name_1,
//	 *	 						fixed_header_name_2 -> user_changed_header_name_2,
//	 * 							fixed_header_name_3 -> user_changed_header_name_3,
//	 * 						}
//	 * 		csv_file_name2 -> {
//	 * 							fixed_header_name_4 -> user_changed_header_name_4,
//	 *	 						fixed_header_name_5 -> user_changed_header_name_5,
//	 * 							fixed_header_name_6 -> user_changed_header_name_6,
//	 * 						} 
//	 * }
//	 */
//	protected Map<String, Map<String, String>> userHeaderNames;
//	
//	protected int count = 0;
//	protected int startRow = 2;
//	protected int maxRows = 2_000_000_000;
//
//	/**
//	 * Specifies which rows in the CSV to load based on user input in the prop file
//	 * @throws FileReaderException 
//	 */
//	public void skipRows() throws IOException {
//		//start count at 1 just row 1 is the header
//		count = 1;
//		if (rdfMap.get("START_ROW") != null) {
//			startRow = Integer.parseInt(rdfMap.get("START_ROW")); 
//		}
//		while( count<startRow-1 && csvHelper.getNextRow() != null)// && count<maxRows)
//		{
//			count++;
//		}
//	}
//	
//	/**
//	 * Load the CSV file
//	 * Gets the headers for each column and reads the property file
//	 * @param fileName String
//	 * @throws FileNotFoundException 
//	 */
//	protected void openCSVFile(final String FILE_LOCATION) {
//		logger.info("Processing csv file: " + FILE_LOCATION);
//
//		// use the csv file helper to load the data
//		csvHelper = new CSVFileHelper();
//		csvHelper.setUsingPropFile(this.propFileDefinedInsideCsv);
//		// assume csv
//		csvHelper.setDelimiter(',');
//		csvHelper.parse(FILE_LOCATION);
//
//		// get the headers for the csv
//		this.header = csvHelper.getHeaders();
//		logger.info("Found headers: " + Arrays.toString(header));
//
//		// such that the thick client works if a prop file is passed in
//		// we also need to adjust for when there are multiple files
//		if(this.propFiles != null) {
//			this.propFile = this.header[this.header.length-1];
//		}
//	}
//
//	protected String[] prepareCsvReader(String fileNames, String customBase, String owlFile, String bdPropFile, String propFile){
//		if(propFile != null && !propFile.isEmpty()) {
//			this.propFileExist = true;
//			this.propFileDefinedInsideCsv = false;
//			this.propFiles = propFile.split(";");
//		} else if(rdfMapArr != null) {
//			this.propFileDefinedInsideCsv = false;
//		}
//		return prepareReader(fileNames, customBase, owlFile, bdPropFile);
//	} 
//	
//	/**
//	 * Closes the CSV file streams
//	 * @throws IOException 
//	 */
//	public void closeCSVFile() {
//		if(csvHelper != null) {
//			csvHelper.clear();
//		}
//	}
//
//	public void setNewCsvHeaders(Map<String, Map<String, String>> newCsvHeaders) {
//		this.userHeaderNames = newCsvHeaders;		
//	}
//	
//	
//}
