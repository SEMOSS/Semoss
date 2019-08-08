package prerna.sablecc2.om;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.IReactor;

public class GenRowStruct {

	// string or number - const
	// column - name of column
	// SQL expression - SQL expression - something that can be done in the realm of SQL
	// Expression - expression that can only be
	
	boolean isAllSQL = true;
	
	public Vector<NounMetadata> vector = new Vector<>();
	
	public void add(Object value, PixelDataType type) {
		if(value instanceof NounMetadata) {
			vector.add((NounMetadata) value);
		} else {
			NounMetadata noun = new NounMetadata(value, type);
			vector.add(noun);
		}
	}
	
	public void add(NounMetadata noun) {
		vector.addElement(noun);
	}
	
	public void addLiteral(String literal) {
		add(literal, PixelDataType.CONST_STRING);
	}
	
	public void addBoolean(Boolean bool) {
		add(bool, PixelDataType.BOOLEAN);
	}
	
	public void addDecimal(Double literal)
	{
		add(literal, PixelDataType.CONST_DECIMAL);
	}
	
	public void addInteger(Integer literal)
	{
		add(literal, PixelDataType.CONST_INT);
	}
	
	public void addColumn(String column)
	{
		add(column.trim(), PixelDataType.COLUMN);
	}
	
	public void addColumn(QueryColumnSelector column)
	{
		add(column, PixelDataType.COLUMN);
	}
	
	public void addMap(Map<Object, Object> map) {
		add(map, PixelDataType.MAP);
	}
	
	public void addComparator(String comparator) {
		add(comparator, PixelDataType.COMPARATOR);
	}
	
	// other than the actual expression
	// I need to run through to find what the input columns are in order for me to run through and add to selectors
	// while, I am doing this for sql expression here, we could replace sql expression and the same story kicks in
	public void addSQLE(String sqlE, String [] inputColumns) {
		add(sqlE, PixelDataType.SQLE);
	}

	public void addE(Expression e) {
		add(e, PixelDataType.E);
		isAllSQL = false;
	}
	
	// this is an operational formula that is being added 
	// imagine the case of if where this could be a full operational formula that needs to be executed
	// however this could be the if part or the else part
	public void addLambda(IReactor reactor) {
		add(reactor, PixelDataType.LAMBDA);
		isAllSQL = false;
	}

	// do a check to find which of these can bbe done through SQL vs. which ones need to happen after
	public boolean isAllSQL() {
		return this.isAllSQL;
	}

	public void addRelation(String leftCol, String joinType, String rightCol) {
		Join join = new Join(leftCol, joinType, rightCol);
		add(join, PixelDataType.JOIN);
	}
	
	public void addRelation(String leftCol, String joinType, String rightCol, String relationshipName) {
		Join join = new Join(leftCol, joinType, rightCol, relationshipName);
		add(join, PixelDataType.JOIN);
	}

	public void merge(GenRowStruct anotherRow) {
		vector.addAll(anotherRow.vector);
	}
	
	/**
	 * Just flush out all the values
	 * @return
	 */
	public List<Object> getAllValues() {
		List<Object> values = new Vector<Object>();
		for(NounMetadata n : this.vector) {
			values.add(n.getValue());
		}
		return values;
	}
	
	// gets all of a particular type
	// works only for columns
	public List<String> getAllColumns()
	{
		List<String> retVector = new ArrayList<>();
		for(NounMetadata noun : vector) {
			if(noun.getNounType() == PixelDataType.COLUMN) {
				retVector.add((String)noun.getValue());
			}
		}
		return retVector;
	}
	
	public List<String> getAllStrValues() {
		List<String> retVector = new ArrayList<>();
		for(NounMetadata noun : vector) {
			if(noun.getNounType() == PixelDataType.COLUMN || noun.getNounType() == PixelDataType.CONST_STRING) {
				retVector.add((String)noun.getValue());
			}
		}
		return retVector;
	}
	
	public List<Join> getAllJoins()
	{
		List<Join> retVector = new ArrayList<>();
		for(NounMetadata noun : vector) {
			if(noun.getNounType() == PixelDataType.JOIN) {
				retVector.add((Join)noun.getValue());
			}
		}
		return retVector;
	}
	
	public List<Object> getValuesOfType(PixelDataType type) {
		List<Object> retVector = new Vector<Object>();
		for(NounMetadata noun : vector) {
			if(noun.getNounType() == type) {
				retVector.add(noun.getValue());
			}
		}
		return retVector;
	}
	
	public List<NounMetadata> getNounsOfType(PixelDataType type) {
		List<NounMetadata> retVector = new Vector<NounMetadata>();
		for(NounMetadata noun : vector) {
			if(noun.getNounType() == type) {
				retVector.add(noun);
			}
		}
		return retVector;
	}
	
	public void removeValuesOfType(PixelDataType type) {
		Iterator<NounMetadata> iterator = vector.iterator();
		while(iterator.hasNext()) {
			NounMetadata noun = iterator.next();
			if(noun.getNounType() == type) {
				iterator.remove();
			}
		}
	}
	
	public List<Object> getAllNumericColumns() {
		List<Object> retVector = new Vector<Object>();
		for(NounMetadata noun : vector) {
			if(noun.getNounType() == PixelDataType.CONST_DECIMAL || 
					noun.getNounType() == PixelDataType.CONST_INT) {
				retVector.add(noun.getValue());
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
	
	public NounMetadata remove(int i) {
		return this.vector.remove(i);
	}
	
	public PixelDataType getMeta(int i) {
		return this.vector.get(i).getNounType();
	}

	// I will turn this into query struct eventually - nope I never will
	public String getColumns() {
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
	
	/**
	 * Retrieve the underlying vector
	 * @return
	 */
	public List<NounMetadata> getVector() {
		return this.vector;
	}

}