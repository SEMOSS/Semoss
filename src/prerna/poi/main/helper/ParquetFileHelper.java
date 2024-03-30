package prerna.poi.main.helper;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import prerna.algorithm.api.SemossDataType;
import prerna.om.HeadersException;

public class ParquetFileHelper {
	
	private static final Logger classLogger = LogManager.getLogger(ParquetFileHelper.class);

	
	// local for helper
	private Logger logger = null;
	
	private String fileLocation = null;
	// we need to keep two sets of headers
	// we will keep the headers as is within the physical file
	private String [] allParquetHeaders = null;
	// ... that is all good and all, but when we have duplicates, it 
	// messes things up. to reduce complexity elsewhere, we will just 
	// create a new unique csv headers string[] to store the values
	// this will in essence become the new "physical names" for each
	// column
	private List<String> newUniqueParquetHeaders = null;
	
	// keep track of integer with values s.t. we can easily reset to get all the values
	// without getting an error when there are duplicate headers within the univocity api
	// this will literally be [0,1,2,3,...,n] where n = number of columns - 1
	private Integer [] headerIntegerArray = null;
	
	// keep track of the current headers being used
	public String [] currHeaders = null;
	
	/*
	 * THIS IS REALLY ANNOYING
	 * In thick client, need to know if the last column is 
	 * the path to the prop file location for csv upload
	 */
	private boolean propFileExists = false;

	// api stores max values for security reasons
	// TODO do I need this for api reads?
	private int maxColumns = 1_000_000;
	private int maxCharsPerColumn = 1_000_000;

	public ParquetFileHelper() {
		
	}
	
	public static boolean isParquetFile(String filePath) {
		String file = filePath.toLowerCase();
		if(file.endsWith(".parquet")) {
			return true;	
		}
		return false;
	}

	/**
	 * Set the logger
	 * @param logger
	 */
	public void setLogger(Logger logger) {
		this.logger = logger;
	}
	
	/**
	 * Parse the new file passed
	 * @param fileLocation		The String location of the fileName
	 */
	public void setFileLocation(String fileLocation) {
		this.fileLocation = fileLocation;
	}

	/**
	 * Return the headers for parquet file
	 * @return
	 */
	public String[] getHeaders() {
		if(this.currHeaders == null) {
			collectHeaders();
			return this.newUniqueParquetHeaders.toArray(new String[this.newUniqueParquetHeaders.size()]);
		}
		return this.currHeaders;
	}
	/**
	 * Return the headers and data valid types of the parquet file
	 * @return
	 * @throws IOException 
	 */
	public static Map<String, SemossDataType> getHeadersAndDataTypes(String fileLocation) throws IOException {
		// declare return object
		Map<String, SemossDataType> headersPlusDataTypes = new LinkedHashMap<>();
		
		// read in the file and get the defined schema
		ParquetFileReader reader = null;
		try {
//			reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fileLocation), new Configuration()));
			reader = ParquetFileReader.open(new LocalInputFile(fileLocation));
			MessageType schema = reader.getFooter().getFileMetaData().getSchema();
	
			// for every column, loop through and grab the data type
	        // Note: don't need to predict here since the type is defined in the meta data
	        int numCols = schema.getFieldCount();
	        String parquetDtype;
	        String [] cols = new String[numCols];
	        SemossDataType [] dataTypes = new SemossDataType[numCols];
	        List<ColumnDescriptor> columns = schema.getColumns();
	        List<String[]> paths = schema.getPaths();
	        boolean[] missingColumns = new boolean[numCols];
	        
	        for (int i = 0; i < schema.getFieldCount(); ++i) {
	        	Type t = schema.getFields().get(i);
	        	  
	        	if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
	        		throw new UnsupportedOperationException("Complex types not supported.");
	        	}
	        	cols[i] = t.getName();
	        	//t.getOriginalType().toString();
	        	parquetDtype = t.asPrimitiveType().getPrimitiveTypeName().toString().toUpperCase();
				switch (parquetDtype) {
				case "BOOLEAN":
					dataTypes[i] = SemossDataType.BOOLEAN;
					break;
				case "DATE":
					dataTypes[i] = SemossDataType.DATE;
					break;
				case "DECIMAL":
					dataTypes[i] = SemossDataType.DOUBLE;
					break;
				case "DOUBLE":
					dataTypes[i] = SemossDataType.DOUBLE;
					break;
				case "FLOAT":
					dataTypes[i] = SemossDataType.DOUBLE;
					break;
				case "TIME":
					dataTypes[i] = SemossDataType.TIMESTAMP;
					break;
				case "TIMESTAMP":
					dataTypes[i] = SemossDataType.TIMESTAMP;
					break;
				case "INT32":
					dataTypes[i] = SemossDataType.INT;
					break;
				case "INT64":
					dataTypes[i] = SemossDataType.INT;
					break;
				default:
					dataTypes[i] = SemossDataType.STRING;
				}        	  
	        	String[] colPath = paths.get(i);
			      
			    if (schema.containsPath(colPath)) {
			        ColumnDescriptor fd = schema.getColumnDescription(colPath);
			        if (!fd.equals(columns.get(i))) {
			        	 throw new UnsupportedOperationException("Schema evolution not supported.");
			         }
			      } else {
			    	  if (columns.get(i).getMaxDefinitionLevel() == 0) {
			          // Column is missing in data but the required data is non-nullable. This file is invalid.
			        	 throw new IOException("Required column is missing in data file. Col: " +
			        			 Arrays.toString(colPath));
			         }
			         missingColumns[i] = true;
			      }
			    headersPlusDataTypes.put(cols[i], dataTypes[i]);
			}
			return headersPlusDataTypes;
		} finally {
			if(reader != null) {
				try {
					reader.close();
				} catch(IOException e) {
					classLogger.error(e.toString());
				}
			}
		}
	}
	
	public void collectHeaders() {
		if(allParquetHeaders == null) {
			
	        ParquetFileReader reader = null;
			try {
//				reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(fileLocation), new Configuration()));
				reader = ParquetFileReader.open(new LocalInputFile(fileLocation));
				MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		        int numCols = schema.getFieldCount();
		        String [] cols = new String[numCols];
		        for (int i = 0; i < schema.getFieldCount(); ++i) {
		        	cols[i] = schema.getFields().get(i).getName();
		        }	
		        allParquetHeaders = cols;
			} catch (IllegalArgumentException | IOException e) {
				classLogger.error(e.toString());
				e.printStackTrace();
			} finally {
				if(reader != null) {
					try {
						reader.close();
					} catch (IOException e) {
						classLogger.error(e.toString());
					}
				}
			}
			
			// need to keep track and make sure our headers are good
			if(allParquetHeaders == null) {
				throw new IllegalArgumentException("No headers found");
			}
			int numCols = allParquetHeaders.length;
			
			// TODO circle back on this
			/*
			 * THIS IS REALLY ANNOYING
			 * In thick client, need to know if the last column is 
			 * the path to the prop file location for csv upload
			 */
			if(propFileExists) {
				numCols--;
			}
			newUniqueParquetHeaders = new Vector<String>(numCols);

			// create the integer array s.t. we can reset the value to get in the future
			headerIntegerArray = new Integer[numCols];
			// grab the headerChecker
			HeadersException headerChecker = HeadersException.getInstance();
			
			for(int colIdx = 0; colIdx < numCols; colIdx++) {
				// just trim all the headers
				allParquetHeaders[colIdx] = allParquetHeaders[colIdx].trim();
				String origHeader = allParquetHeaders[colIdx];
				if(origHeader.trim().isEmpty()) {
					origHeader = "BLANK_HEADER";
				}
				String newHeader = headerChecker.recursivelyFixHeaders(origHeader, newUniqueParquetHeaders);

				// now update the unique headers, as this will be used to match duplications
				newUniqueParquetHeaders.add(newHeader);

				// fill in integer array
				headerIntegerArray[colIdx] = colIdx;
			}
		}
	}
	
	public void modifyCleanedHeaders(Map<String, String> thisFileHeaderChanges) {
		// iterate through all sets of oldHeader -> newHeader
		collectHeaders();
		for(String desiredNewHeaderValue : thisFileHeaderChanges.keySet()) {
			String oldHeader = thisFileHeaderChanges.get(desiredNewHeaderValue);
			
			// since the user may not want all the headers, we only check if new headers are valid
			// based on the headers they want
			// thus, we need to check and see if the newHeaderValue is actually already used
			int newNameIndex = this.newUniqueParquetHeaders.indexOf(desiredNewHeaderValue);
			if(newNameIndex >= 0) {
				// this new header exists
				// lets modify it
				this.newUniqueParquetHeaders.set(newNameIndex, "NOT_USED_COLUMN_1234567890");
			}
			
			// now we modify what was the old header to be the new header
			int oldHeaderIndex = this.newUniqueParquetHeaders.indexOf(oldHeader);
			this.newUniqueParquetHeaders.set(oldHeaderIndex, desiredNewHeaderValue);
		}
	}
	
	/**
	 * Set a limit on which columns you want to be parsed
	 * @param columns			The String[] containing the headers you want
	 */
	public void parseColumns(String[] columns) {
		// map it back to clean columns
		// TODO do I need this for security?
		//makeSettings();
		if (newUniqueParquetHeaders==null) {
			collectHeaders();
		}
		// must use index for when there are duplicate values
		Integer[] values = new Integer[columns.length];
		for(int colIdx = 0; colIdx < columns.length; colIdx++) {
			values[colIdx] = newUniqueParquetHeaders.indexOf(columns[colIdx]);
		}
		//settings.selectIndexes(values);
		currHeaders = columns;
		
		// TODO check if I need this because Im not changing order unlike file parser
		//reset(false);
	}
	
	public List<String> getNewUniqueParquetHeaders(){
		return this.newUniqueParquetHeaders;
	}
}
