package prerna.ds.r;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.util.Utility;

public class RIterator implements Iterator<IHeadersDataRow>{

	private static final Logger LOGGER = LogManager.getLogger(RIterator.class.getName());
	
	private RFrameBuilder builder;
	private SelectQueryStruct qs;

	private String tempVarName;
	private int numRows;

	private String[] headers = null;
	private String[] colTypes = null;

	private List<Object[]> data;
	private int dataPos = 0;
	private int rowIndex = 1;
	private int bulkRowSize = 10_000;

	public RIterator(RFrameBuilder builder, String rQuery, SelectQueryStruct qs) {
		this.builder = builder;
		this.qs = qs;
		//Validate user input won't break R and crash JVM
		RregexValidator reg = new RregexValidator();
		reg.Validate(rQuery);

		long start = System.currentTimeMillis();

		this.tempVarName = "temp" + Utility.getRandomString(6);
		String tempVarQuery = this.tempVarName + " <- {" + rQuery + "}";
		this.builder.evalR(tempVarQuery);
		this.numRows = builder.getNumRows(this.tempVarName);
		
		// need to account for limit and offset
		long limit = qs.getLimit();
		long offset = qs.getOffset();
		if(offset > numRows) {
			// well, no point in doing anything else
			this.numRows = 0;
		} else if(limit > 0 || offset > 0) {
			// java is 0 based so the FE sends 0 when they want the first record
			// but R is 1 based, so we need to add 1 to the offset value
			String updatedTempVarQuery = addLimitOffset(this.tempVarName, this.numRows, limit, offset);
			this.builder.evalR(updatedTempVarQuery);
			// and then update the number of rows
			this.numRows = builder.getNumRows(this.tempVarName);
		}
		
		
		long end = System.currentTimeMillis();
		LOGGER.info("TIME TO EXECUTE MAIN R SCRIPT = " + (end-start) + "ms");
		
		//obtain headers from the qs
		List<Map<String, Object>> headerInfo = qs.getHeaderInfo();
		int numCols = headerInfo.size();
		this.headers = new String[numCols];
		for (int i = 0; i <numCols; i++) {
			headers[i] = headerInfo.get(i).get("alias").toString();
		}
		this.colTypes = builder.getColumnTypes(tempVarName);
	}

	public RIterator(RFrameBuilder builder, String rQuery) {
		this.builder = builder;

		long start = System.currentTimeMillis();

		this.tempVarName = "temp" + Utility.getRandomString(6);
		String tempVarQuery = this.tempVarName + " <- {" + rQuery + "}";
		this.builder.evalR(tempVarQuery);
		this.numRows = builder.getNumRows(this.tempVarName);
		
		// need to account for limit and offset		
		
		long end = System.currentTimeMillis();
		LOGGER.info("TIME TO EXECUTE MAIN R SCRIPT = " + (end-start) + "ms");
		
		this.headers = builder.getColumnNames(tempVarName);
		this.colTypes = builder.getColumnTypes(tempVarName);
	}

	private String addLimitOffset(String tempVarQuery, int numRows, long limit, long offset) {
		StringBuilder query = new StringBuilder(tempVarQuery);
		query.append(" <- ").append(tempVarQuery);
		if(limit > 0) {
			if(offset > 0) {
				// we have limit + offset
				long lastRIndex = offset + limit;
				// r is 1 based so we will increase the offset by 1
				// since FE sends back limit/offset 0 based
				offset++;
				if(numRows < lastRIndex) {
					query.append("[").append(offset).append(":").append(numRows).append("]");
				} else {
					query.append("[").append(offset).append(":").append((lastRIndex)).append("]");
				}
			} else {
				// we just have a limit
				if(numRows < limit) {
					query.append("[1:").append(numRows).append("]");
				} else {
					query.append("[1:").append(limit).append("]");
				}
			}
		} else if(offset > 0) {
			// r is 1 based so we will increase the offset by 1
			// since FE sends back limit/offset 0 based
			offset++;
			query.append("[").append(offset).append(":").append(numRows).append("]");
		}
		return query.toString();
	}

	@Override
	public boolean hasNext() {
		if (rowIndex <= this.numRows) {
			return true;
		} else {
			this.builder.evalR("rm(" + this.tempVarName + ")");
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
			LOGGER.debug("TIME TO GET SUBSET OF R VALUES = " + (endT-startT) + "ms");
			
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
	
	public int getNumRows() {
		return this.numRows;
	}

}