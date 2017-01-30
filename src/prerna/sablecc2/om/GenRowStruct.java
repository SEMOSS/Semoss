package prerna.sablecc2.om;

import java.util.Vector;

public class GenRowStruct extends Vector {

	// string or number - const
	// column - name of column
	// SQL expression - SQL expression - something that can be done in the realm of SQL
	// Expression - expression that can only be
	public enum COLUMN_TYPE {CONST_DECIMAL, CONST_STRING, COLUMN, SQLE, E, FILTER, JOIN}; 
	
	boolean isAllSQL = true;
	
	public Vector<COLUMN_TYPE> metaVector = new Vector<COLUMN_TYPE>();
	String columns = "";
	
	public void addLiteral(String literal)
	{
		super.addElement(literal);
		metaVector.add(COLUMN_TYPE.CONST_STRING);
	}
	
	public void addDecimal(Double literal)
	{
		super.addElement(literal);
		metaVector.add(COLUMN_TYPE.CONST_DECIMAL);
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

	public void addFilter(Join join)
	{
		super.addElement(join);
		metaVector.add(COLUMN_TYPE.FILTER);		
		
		// add this to set of selectors as well. Not sure I need to
		//super.addElement(join.getSelector());
		//metaVector.add(COLUMN_TYPE.COLUMN);		
	}
	

	public void addRelation(String leftCol, String joinType, String rightCol)
	{
		Join filter = new Join(leftCol, joinType, rightCol);
		super.addElement(filter);
		metaVector.add(COLUMN_TYPE.JOIN);
		
		// I also need to add these to columns
		// should I parse out the table ?
		// hmm.. then it becomes an issue
		// not sure I need this right now
		/*super.addElement(leftCol);
		metaVector.add(COLUMN_TYPE.COLUMN);
		super.addElement(rightCol);
		metaVector.add(COLUMN_TYPE.COLUMN);*/
	}

	public void merge(GenRowStruct anotherRow)
	{
		super.addAll(anotherRow);
		metaVector.addAll(anotherRow.metaVector);
	}
	
	// gets all of a particular type
	// works only for columns
	public Vector<String> getAllColumns()
	{
		Vector<String> retVector = new Vector<String>();
		for(int elementIndex = 0;elementIndex < metaVector.size();elementIndex++)
		{
			if(metaVector.elementAt(elementIndex) == COLUMN_TYPE.COLUMN)
				retVector.add(this.elementAt(elementIndex)+"");
		}
		return retVector;
	}
	
	public Vector<Object> getColumnsOfType(COLUMN_TYPE type)
	{
		Vector<Object> retVector = new Vector<Object>();
		for(int elementIndex = 0;elementIndex < metaVector.size();elementIndex++)
		{
			if(metaVector.elementAt(elementIndex) == type)
				retVector.add(this.elementAt(elementIndex));
		}
		return retVector;
	}

	// I will turn this into query struct eventually - nope I never will
	
	
	
	public String getColumns()
	{
		return columns;
	}
	
	
}