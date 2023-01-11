package prerna.ds.r;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.Utility;

public class RIterator implements Iterator<IHeadersDataRow>{

	private static final Logger logger = LogManager.getLogger(RIterator.class);
	
	private boolean init = false;
	
	private RFrameBuilder builder;
	private SelectQueryStruct qs;
	private String rQuery;
	
	private String tempVarName;
	private int totalNumRows;
	private int numRows;

	private String[] headers = null;
	private String[] colTypes = null;

	private List<Object[]> data;
	private int dataPos = 0;
	private int rowIndex = 1;
	private int bulkRowSize = 10_000;
	
	// since dates are turned to strings to send to FE
	// need to maintain so we return the type as expected
	private Map<String, SemossDataType> convertedDates;
	
	private List<String> varsToCleanup = new ArrayList<>();
	
	// main query instead of the whole thing for lookup
	private String query = null;

	public RIterator(RFrameBuilder builder, String rQuery, SelectQueryStruct qs) {
		this.builder = builder;
		this.qs = qs;
		this.rQuery = rQuery;
		init();
	}

	public RIterator(RFrameBuilder builder, String rQuery) {
		this.builder = builder;
		this.rQuery = rQuery;
		init();
	}
	
	private void init() {
		if (init) {
			return;
		}

		synchronized (this) {
			if(!init) {
				long start = System.currentTimeMillis();

				this.tempVarName = "temp" + Utility.getRandomString(6);
				String tempVarQuery = this.tempVarName + " <- {" + rQuery + "}";
				this.builder.evalR(tempVarQuery);
				this.totalNumRows = builder.getNumRows(this.tempVarName);
				this.numRows = this.totalNumRows;
				
				varsToCleanup.add(this.tempVarName);
				
				// need to account for limit and offset
				if(this.qs != null && this.numRows > 0) {
					long limit = qs.getLimit();
					long offset = qs.getOffset();
					if(offset > numRows) {
						// well, no point in doing anything else
						this.numRows = 0;
					} else if(limit > 0 || offset > 0) {
						// java is 0 based so the FE sends 0 when they want the first record
						// but R is 1 based, so we need to add 1 to the offset value
						String updatedTempVarQuery = null;
						try {
							updatedTempVarQuery = RSyntaxHelper.determineLimitOffsetSyntax(this.tempVarName, this.numRows, limit, offset);
							this.builder.evalR(updatedTempVarQuery);
							// and then update the number of rows
							this.numRows = builder.getNumRows(this.tempVarName);
						} catch(IllegalArgumentException e) {
							// we have no data
							// will still run with a limit of 1 so that 
							// we can grab the metadata
							updatedTempVarQuery = RSyntaxHelper.determineLimitOffsetSyntax(this.tempVarName, this.numRows, 1, 0);
							this.builder.evalR(updatedTempVarQuery);
							// and then update the number of rows
							this.numRows = 0;
						}
					}
				}
				
				long end = System.currentTimeMillis();
				logger.info("TIME TO EXECUTE MAIN R SCRIPT = " + (end-start) + "ms");
				
				//obtain headers from the qs
				if(this.qs != null && !(this.qs instanceof HardSelectQueryStruct)) {
					List<Map<String, Object>> headerInfo = qs.getHeaderInfo();
					int numCols = headerInfo.size();
					this.headers = new String[numCols];
					for (int i = 0; i <numCols; i++) {
						headers[i] = headerInfo.get(i).get("alias").toString();
					}
				} else {
					this.headers = builder.getColumnNames(tempVarName);
				}
				this.colTypes = builder.getColumnTypes(tempVarName + "[," + RSyntaxHelper.createStringRColVec(headers) + "]");
				
				// init is true
				init = true;
			}
		}
	}

	@Override
	public boolean hasNext() {
		if (rowIndex <= this.numRows) {
			return true;
		} else {
			cleanUp();
			return false;
		}
	}

	@Override
	public IHeadersDataRow next() {
		//store the values in a list of objects
		Object[] values = null;

		// if the data is null or the data position has reached the total size of the data
		if(this.data == null || this.dataPos + 1 > this.data.size()) {
			// rowIndex starts at 1; make it the same as the bulkRowSize
			int end = this.rowIndex + this.bulkRowSize - 1;
			if (end > this.numRows) {
				end = this.numRows;
			}

			//account for if there is a single row to output
			if (rowIndex == end) {
				values = this.builder.getDataRow(this.tempVarName + "[" + this.rowIndex + "]", headers);
				this.rowIndex++;
				this.dataPos++;
				IHeadersDataRow row = new HeadersDataRow(headers, values);
				return row;
			}

			//execute the rScript based on the headers
			long startT = System.currentTimeMillis();
			String query = this.tempVarName + "[" + this.rowIndex + ":" + end + "]";
			this.data = this.builder.getBulkDataRow(query, headers);
			long endT = System.currentTimeMillis();
			logger.debug("TIME TO GET SUBSET OF R VALUES = " + (endT-startT) + "ms");
			
			this.dataPos = 0;
		}

		//we are grabbing a single row of values - always at the next row index
		values = data.get(dataPos);

		this.rowIndex++;
		this.dataPos++;

		IHeadersDataRow row = new HeadersDataRow(headers, values);
		return row;
	}

	public String[] getHeaders() {
		return headers;
	}

	public void setHeaders(String[] headers) {
		this.headers = headers;
	}

	public String[] getColTypes() {
		return colTypes;
	}

	public void setColTypes(String[] colTypes) {
		this.colTypes = colTypes;
	}
	
	public void setConvertedDates(Map<String, SemossDataType> convertedDates) {
		this.convertedDates = convertedDates;
		if(this.convertedDates != null && !this.convertedDates.isEmpty()) {
			String[] headers = getHeaders();
			String[] types = getColTypes();
			int size = headers.length;
			for(int i = 0; i < size; i++) {
				if(convertedDates.containsKey(headers[i])) {
					types[i] = convertedDates.get(headers[i]).toString();
				}
			}
		}
	}
	
	public int getTotalNumRows() {
		return this.totalNumRows;
	}

	public SelectQueryStruct getQs() {
		return this.qs;
	}
	
	public String getQuery() {
		return this.query;
	}
	
	public void setQuery(String query) {
		this.query = query;
	}
	
	public void addVarForCleanup(String var) {
		this.varsToCleanup.add(var);
	}
	
	public void cleanUp() {
		StringBuilder builder = new StringBuilder();
		for(String var : this.varsToCleanup) {
			builder.append("rm(").append(var).append(");");
		}
		builder.append("gc();");
		this.builder.evalR(builder.toString());
	}
	
	public String getTempVarName() {
		return this.tempVarName;
	}

	public String getJsonOfResults() {
		// TODO: writing to file? 
		// write(toJSON(dt, dataframe=c("values"), pretty=TRUE), "c:/workspace/testing.json")
		return this.builder.getRJavaTranslator().getString(
				"jsonlite:::toJSON( " + this.tempVarName  + "[," + RSyntaxHelper.createStringRColVec(headers) + "], "
						+ "dataframe=c('values'), factor=c('string'), na=c(NULL) )");
	}
}