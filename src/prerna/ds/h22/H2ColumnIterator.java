package prerna.ds.h22;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class H2ColumnIterator implements Iterator<Object>{

	ResultSet resultSet;
	Object nextValue;
	
	public H2ColumnIterator(ResultSet resultSet) {
		this.resultSet = resultSet;
		if(this.resultSet != null){
			nextValue = getNextValue();
		}
	}
	
	@Override
	public boolean hasNext() {
		return nextValue != null;
	}

	@Override
	public Object next() {
		if(hasNext()) {
			Object value = nextValue;
			nextValue = getNextValue();
			return value;
		} else {
			throw new NoSuchElementException("No more elements");
		}
	}
	
	private Object getNextValue() {
    	Object value = null;
		try {
	        if (resultSet.next()){
	            value = resultSet.getObject(1);
	        } else {
	        	// make sure we close of the stream
	        	resultSet.close();
	        }
	        return value;			
		} catch (SQLException e) {
			return null;
		}    	
	}
}
