package prerna.rdf.engine.wrappers;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Hashtable;

import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;

public class RDBMSSelectWrapper extends AbstractWrapper implements
		ISelectWrapper {

	public static String uri = "http://semoss.org/ontologies/concept";
	ResultSet rs = null;
	boolean hasMore = false;
	Hashtable columnTypes = new Hashtable();

	@Override
	public void execute() {
		// TODO Auto-generated method stub
		rs = (ResultSet)engine.execSelectQuery(query);
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		try {
			hasMore =  rs.next();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return hasMore;
	}

	@Override
	public ISelectStatement next() {
		// TODO Auto-generated method stub
		ISelectStatement stmt = new SelectStatement();
		try {
			for(int colIndex = 0;colIndex < var.length;colIndex++)
			{
				Object value = rs.getObject(var[colIndex]);
				stmt.setVar(var[colIndex], value);
				//set the type and URI based on the type
				int type = (int)columnTypes.get(var[colIndex]);
				String qualifiedValue = var[colIndex];
				if(type == Types.LONGNVARCHAR || type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGNVARCHAR || type == Types.NCHAR)
					qualifiedValue = uri + "/" + var[colIndex] + "/" + value;
				stmt.setRawVar(var[colIndex], qualifiedValue);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return stmt;
	}

	@Override
	public String[] getVariables() {
		
		if(var != null)
			return var;
		// get the result set metadata to get the column names
		try {
			ResultSetMetaData rsmd = rs.getMetaData();
			int numColumns = rsmd.getColumnCount();
			
			var = new String[numColumns-1];
			
			for(int colIndex = 1;colIndex < numColumns;colIndex++)
			{
				var[colIndex-1] = rsmd.getColumnName(colIndex);
				int type = rsmd.getColumnType(colIndex);
				columnTypes.put(var[colIndex-1], type);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return var;
	}
	
	private Object typeConverter(Object input, int type)
	{
		Object retObject = input;
		switch(type)
		{
			
		// to be implemented later
		
		}
		
		return retObject;
	}
}
