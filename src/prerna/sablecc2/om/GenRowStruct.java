package prerna.sablecc2.om;

import java.util.Vector;
public class GenRowStruct {

	// string or number - const
	// column - name of column
	// SQL expression - SQL expression - something that can be done in the realm of SQL
	// Expression - expression that can only be
	
	boolean isAllSQL = true;
	
	public Vector<Object> vector = new Vector<Object>();
	public Vector<PkslDataTypes> metaVector = new Vector<PkslDataTypes>();
	String columns = "";
	
	public void add(Object value, PkslDataTypes type) {
		vector.addElement(value);
		metaVector.add(type);
	}
	
	public void addLiteral(String literal)
	{
		vector.addElement(literal);
		metaVector.add(PkslDataTypes.CONST_STRING);
	}
	
	public void addDecimal(Double literal)
	{
		vector.addElement(literal);
		metaVector.add(PkslDataTypes.CONST_DECIMAL);
	}
	
	public void addColumn(String column)
	{
		column = column.trim();
		vector.addElement(column);
		metaVector.add(PkslDataTypes.COLUMN);
		columns = columns + "_" + column;
	}
	
	// other than the actual expression
	// I need to run through to find what the input columns are in order for me to run through and add to selectors
	// while, I am doing this for sql expression here, we could replace sql expression and the same story kicks in
	public void addSQLE(String sqlE, String [] inputColumns)
	{
		vector.addElement(sqlE);
		metaVector.add(PkslDataTypes.SQLE);
	}

	public void addE(String E, String [] inputColumns)
	{
		vector.addElement(E);
		metaVector.add(PkslDataTypes.E);
		isAllSQL = false;
	}

	// do a check to find which of these can bbe done through SQL vs. which ones need to happen after
	public boolean isAllSQL()
	{
		return this.isAllSQL;
	}

	public void addFilter(Filter filter)
	{
		vector.addElement(filter);
		metaVector.add(PkslDataTypes.FILTER);		
	}
	
	public void addRelation(String leftCol, String joinType, String rightCol)
	{
		Join join = new Join(leftCol, joinType, rightCol);
		vector.addElement(join);
		metaVector.add(PkslDataTypes.JOIN);
	}

	public void merge(GenRowStruct anotherRow)
	{
		vector.addAll(anotherRow.vector);
		metaVector.addAll(anotherRow.metaVector);
	}
	
	// gets all of a particular type
	// works only for columns
	public Vector<String> getAllColumns()
	{
		Vector<String> retVector = new Vector<String>();
		for(int elementIndex = 0;elementIndex < metaVector.size();elementIndex++)
		{
			if(metaVector.elementAt(elementIndex) == PkslDataTypes.COLUMN)
				retVector.add(vector.elementAt(elementIndex)+"");
		}
		return retVector;
	}
	
	public Vector<Object> getColumnsOfType(PkslDataTypes type)
	{
		Vector<Object> retVector = new Vector<Object>();
		for(int elementIndex = 0;elementIndex < metaVector.size();elementIndex++)
		{
			if(metaVector.elementAt(elementIndex) == type)
				retVector.add(vector.elementAt(elementIndex));
		}
		return retVector;
	}
	
	public int size() {
		return this.vector.size();
	}
	
	public Object get(int i) {
		return this.vector.get(i);
	}
	
	public PkslDataTypes getMeta(int i) {
		return this.metaVector.get(i);
	}

	// I will turn this into query struct eventually - nope I never will
	
	
	
	public String getColumns()
	{
		return columns;
	}
	
	public String getDataString() {
		String s = "";
		s += "META VECTOR: "+this.metaVector+"\n";
		s += "DATA VECTOR: "+this;
		return s;
	}
}