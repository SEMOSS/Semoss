package prerna.ds.h2;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class H2Iterator implements Iterator<Object[]>{

	ResultSet resultSet;
	String[] headers;
	String[] types;
	Object[] nextRow;
	
	public H2Iterator(ResultSet resultSet) {
		this.resultSet = resultSet;
		if(this.resultSet != null){
			nextRow = getNextRow();
		}
		setHeaders();
	}
	
	@Override
	public boolean hasNext() {
		return nextRow != null;
	}

	@Override
	public Object[] next() {
		
		if(hasNext()) {
			Object[] row = nextRow;
			nextRow = getNextRow();
			return row;
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
//	            		row[i-1] = "null";
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
	
	public String[] getHeaders() {
		if(this.headers == null) {
			setHeaders();
		}
		return this.headers;
	}
	
	public String[] getTypes() {
		if(types == null) {
			try {
				ResultSetMetaData rsmd = resultSet.getMetaData();
		        int NumOfCol = rsmd.getColumnCount();
		        types = new String[NumOfCol];
		        for(int i = 1; i <= NumOfCol; i++) {
		        	types[i-1] = rsmd.getColumnTypeName(i);
	            }
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
		return types;
	}
	
	private void setHeaders() {
		ResultSetMetaData rsmd;
		try {
			rsmd = this.resultSet.getMetaData();
			int columnCount = rsmd.getColumnCount();
			this.headers = new String[columnCount];
			for (int i = 1; i <= columnCount; i++ ) {
				headers[i-1] = rsmd.getColumnName(i);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
