package prerna.sablecc2.om;

import java.util.Vector;

public class GenRowStruct extends Vector {

	// string or number - const
	// column - name of column
	// SQL expression - SQL expression - something that can be done in the realm of SQL
	// Expression - expression that can only be
	public enum COLUMN_TYPE {CONST, COLUMN, SQLE, E, FILTER}; 
	
	boolean isAllSQL = true;
	
	public Vector<COLUMN_TYPE> metaVector = new Vector<COLUMN_TYPE>();
	String columns = "";
	
	public void addLiteral(String literal)
	{
		super.addElement(literal);
		metaVector.add(COLUMN_TYPE.CONST);
	}
	
	public void addDecimal(Double literal)
	{
		super.addElement(literal);
		metaVector.add(COLUMN_TYPE.CONST);
	}
	
	public void addColumn(String column)
	{
		column = column.trim();
		super.addElement(column);
		metaVector.add(COLUMN_TYPE.COLUMN);
		columns = columns + "_" + column;
	}
	
	// other than the actual expression
	// I need to run through to find what the input columns are in order for me to run through and add to selectors
	// while, I am doing this for sql expression here, we could replace sql expression and the same story kicks in
	public void addSQLE(String sqlE, String [] inputColumns)
	{
		super.addElement(sqlE);
		metaVector.add(COLUMN_TYPE.SQLE);
	}

	public void addE(String E, String [] inputColumns)
	{
		super.addElement(E);
		metaVector.add(COLUMN_TYPE.E);
		isAllSQL = false;
	}

	// do a check to find which of these can bbe done through SQL vs. which ones need to happen after
	public boolean isAllSQL()
	{
		return this.isAllSQL;
	}
	
	public void addFilter(String filter)
	{
		super.addElement(filter);
		metaVector.add(COLUMN_TYPE.FILTER);
	}
	
	public void merge(GenRowStruct anotherRow)
	{
		super.addAll(anotherRow);
		metaVector.addAll(anotherRow.metaVector);
	}
	
	// gets all of a particular type
	public Vector<String> getType(COLUMN_TYPE type)
	{
		Vector<String> retVector = new Vector<String>();
		for(int elementIndex = 0;elementIndex < metaVector.size();elementIndex++)
		{
			if(metaVector.elementAt(elementIndex) == type)
				retVector.add(this.elementAt(elementIndex)+"");
		}
		return retVector;
	}
	// I will turn this into query struct eventually
	
	public String getColumns()
	{
		return columns;
	}
}