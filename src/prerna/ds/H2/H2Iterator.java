package prerna.ds.H2;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class H2Iterator implements Iterator<Object[]>{

	ResultSet resultSet;
	Object[] nextRow;
	
	public H2Iterator(ResultSet resultSet) {
		this.resultSet = resultSet;
		if(this.resultSet != null){
			nextRow = getNextRow();
		}
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
	            	if(resultSet.getObject(i) != null)//added to display blank instead of null on the FE
	            		row[i-1] = resultSet.getObject(i);
	            	else
	            		row[i-1] = "";
	            }
	        }
	        return row;			
		} catch (SQLException e) {
			return null;
		}    	
	}
}
