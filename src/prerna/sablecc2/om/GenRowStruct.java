package prerna.sablecc2.om;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import prerna.sablecc2.reactor.IReactor;

public class GenRowStruct {

	// string or number - const
	// column - name of column
	// SQL expression - SQL expression - something that can be done in the realm of SQL
	// Expression - expression that can only be
	
	boolean isAllSQL = true;
	
	public Vector<NounMetadata> vector = new Vector<>();
	
	public void add(Object value, PkslDataTypes type) {
		if(value instanceof NounMetadata) {
			add((NounMetadata)value);
		} else {
			NounMetadata noun = new NounMetadata(value, type);
			vector.add(noun);
		}
	}
	
	public void add(NounMetadata noun) {
		vector.addElement(noun);
	}
	
	public void addLiteral(String literal)
	{
		add(literal, PkslDataTypes.CONST_STRING);
	}
	
	public void addDecimal(Double literal)
	{
		add(literal, PkslDataTypes.CONST_DECIMAL);
	}
	
	public void addInteger(Integer literal)
	{
		add(literal, PkslDataTypes.CONST_INT);
	}
	
	public void addColumn(String column)
	{
		add(column.trim(), PkslDataTypes.COLUMN);
//		columns = columns + "_" + column;
	}
	
	// other than the actual expression
	// I need to run through to find what the input columns are in order for me to run through and add to selectors
	// while, I am doing this for sql expression here, we could replace sql expression and the same story kicks in
	public void addSQLE(String sqlE, String [] inputColumns)
	{
		add(sqlE, PkslDataTypes.SQLE);
	}

	public void addE(Expression e)
	{
		add(e, PkslDataTypes.E);
		isAllSQL = false;
	}
	
	// this is an operational formula that is being added 
	// imagine the case of if where this could be a full operational formula that needs to be executed
	// however this could be the if part or the else part
	public void addLambda(IReactor reactor)
	{
		add(reactor, PkslDataTypes.LAMBDA);
		isAllSQL = false;
		
	}

	// do a check to find which of these can bbe done through SQL vs. which ones need to happen after
	public boolean isAllSQL()
	{
		return this.isAllSQL;
	}

	public void addFilter(Filter filter)
	{
		add(filter, PkslDataTypes.FILTER);
	}
	
	public void addRelation(String leftCol, String joinType, String rightCol)
	{
		Join join = new Join(leftCol, joinType, rightCol);
		add(join, PkslDataTypes.JOIN);
	}

	public void merge(GenRowStruct anotherRow)
	{
		vector.addAll(anotherRow.vector);
	}
	
	// gets all of a particular type
	// works only for columns
	public List<String> getAllColumns()
	{
		List<String> retVector = new ArrayList<>();
		for(NounMetadata noun : vector) {
			if(noun.getNounName() == PkslDataTypes.COLUMN) {
				retVector.add((String)noun.getValue());
			}
		}
		return retVector;
	}
	
	public List<Object> getColumnsOfType(PkslDataTypes type)
	{
		List<Object> retVector = new Vector<Object>();
		for(NounMetadata noun : vector) {
			if(noun.getNounName() == type) {
				retVector.add(noun.getValue());
			}
		}
		return retVector;
	}
	
	public List<Object> getAllNumericColumns() {
		List<Object> retVector = new Vector<Object>();
		for(NounMetadata noun : vector) {
			if(noun.getNounName() == PkslDataTypes.CONST_DECIMAL || 
					noun.getNounName() == PkslDataTypes.CONST_INT) {
				retVector.add(noun.getValue());
			}
		}
		return retVector;
	}
	
	
	public List<NounMetadata> getNounsOfType(PkslDataTypes type)
	{
		List<NounMetadata> retVector = new Vector<NounMetadata>();
		for(NounMetadata noun : vector) {
			if(noun.getNounName() == type) {
				retVector.add(noun);
			}
		}
		return retVector;
	}
	
	public int size() {
		return this.vector.size();
	}
	
	public Object get(int i) {
		return this.vector.get(i).getValue();
	}
	
	public NounMetadata getNoun(int i) {
		return this.vector.get(i);
	}
	
	public NounMetadata getLastNoun() {
		return this.vector.get(this.vector.size()-1);
	}
	
	public void replaceNounMetaAtIndex(int index, NounMetadata newNoun) {
		this.vector.remove(index);
		this.vector.add(index, newNoun);
	}
	
	public PkslDataTypes getMeta(int i) {
		return this.vector.get(i).getNounName();
	}

	// I will turn this into query struct eventually - nope I never will
	
	public String getColumns()
	{
//		return columns;
		return "";
	}
	
	/**
	 * Override to string method for easier debugging
	 */
	public String toString() {
		return this.vector.toString();
	}

	public boolean isEmpty() {
		return this.vector.isEmpty();
	}

}