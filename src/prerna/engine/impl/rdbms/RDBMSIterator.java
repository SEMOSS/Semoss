package prerna.engine.impl.rdbms;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdf.HeadersDataRow;

public class RDBMSIterator implements Iterator<IHeadersDataRow>{

	ResultSet resultSet;
	Object[] nextRow;
	String[] headers;
	
	
	public RDBMSIterator(ResultSet resultSet) {
		this.resultSet = resultSet;
		if(this.resultSet != null){
			headers = getHeaders();
			nextRow = getNextRow();
		}
	}
	
	private String[] getHeaders() {
		try {
			ResultSetMetaData metaData = resultSet.getMetaData();
			int colCount = metaData.getColumnCount();
			String[] headers = new String[colCount];
			for(int i = 1; i <= colCount; i++) {
				headers[i-1] = metaData.getColumnName(i);
			}
			return headers;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new String[]{};
	}
	
	@Override
	public boolean hasNext() {
		return nextRow != null;
	}

	@Override
	public HeadersDataRow next() {
		
		if(hasNext()) {
			Object[] row = nextRow;
			nextRow = getNextRow();
			HeadersDataRow retData = new HeadersDataRow(headers, row, row);
			return retData;
		} else {
			throw new NoSuchElementException("No more elements");
		}

	}
	
	private Object[] getNextRow() {
    	Object[] row = null;
		try {
			ResultSetMetaData rsmd = resultSet.getMetaData();
	        int NumOfCol = rsmd.getColumnCount();
	        if (resultSet.next()){
	            row = new Object[NumOfCol];
	            for(int i = 1; i <= NumOfCol; i++) {
//	            	if(resultSet.getObject(i) != null)//added to display blank instead of null on the FE
	            		row[i-1] = resultSet.getObject(i);
//	            	else
//	            		row[i-1] = "";
	            }
	        } else {
	        	// make sure we close of the stream
	        	resultSet.close();
	        }
	        return row;			
		} catch (SQLException e) {
			return null;
		}    	
	}
}
