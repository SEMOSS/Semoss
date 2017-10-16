package prerna.ds.r;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.om.HeadersDataRow;
import prerna.query.querystruct.QueryStruct2;
import prerna.util.Utility;

public class RIterator2 implements Iterator<IHeadersDataRow>{

	private static final Logger LOGGER = LogManager.getLogger(RIterator2.class.getName());
	
	private AbstractRBuilder builder;
	private QueryStruct2 qs;

	private String tempVarName;
	private int numRows;

	private String[] headers = null;

	private List<Object[]> data;
	private int dataPos = 0;
	private int rowIndex = 1;
	private int bulkRowSize = 5000;

	public RIterator2(AbstractRBuilder builder, String rQuery, QueryStruct2 qs) {
		this.builder = builder;
		this.qs = qs;

		long start = System.currentTimeMillis();

		this.tempVarName = "temp" + Utility.getRandomString(6);
		String tempVarQuery = this.tempVarName + " <- {" + rQuery + "}";
		this.builder.executeR(tempVarQuery);
		this.numRows = builder.getNumRows(this.tempVarName);
		
		// need to account for limit and offset
		long limit = qs.getLimit();
		long offset = qs.getOffset();
		if(offset > numRows) {
			// well, no point in doing anything else
			this.numRows = 0;
		} else if(limit > 0 || offset > 0) {
			String updatedTempVarQuery = addLimitOffset(this.tempVarName, this.numRows, limit, offset);
			this.builder.executeR(updatedTempVarQuery);
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
	}
	
	private String addLimitOffset(String tempVarQuery, int numRows, long limit, long offset) {
		StringBuilder query = new StringBuilder(tempVarQuery);
		query.append(" <- ").append(tempVarQuery);
		if(limit > 0) {
			if(offset > 0) {
				// we have limit + offset
				long lastRIndex = offset + limit;
				if(numRows < lastRIndex) {
					query.append("[").append(offset).append(":").append(numRows).append("]");
				} else {
					query.append("[").append(offset).append(":").append((offset + limit)).append("]");
				}
			} else {
				// we just have a limit
				if(numRows < limit) {
					query.append("[0:").append(numRows).append("]");
				} else {
					query.append("[0:").append(limit).append("]");
				}
			}
		} else if(offset > 0) {
			// we just have offset
			query.append("[").append(offset).append(":").append(numRows).append("]");
		}
		return query.toString();
	}

	@Override
	public boolean hasNext() {
		if (rowIndex <= this.numRows) {
			return true;
		} else {
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
			LOGGER.info("TIME TO GET SUBSET OF R VALUES = " + (endT-startT) + "ms");
			
			this.dataPos = 0;
		}

		//we are grabbing a single row of values - always at the next row index
		values = data.get(dataPos);

		this.rowIndex++;
		this.dataPos++;

		IHeadersDataRow row = new HeadersDataRow(headers, values);
		return row;
	}

}