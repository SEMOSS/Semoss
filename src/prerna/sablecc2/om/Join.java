package prerna.sablecc2.om;

import java.util.Vector;

// left column
// right column
// type of join
public class Join {

	//public enum JOIN_TYPE {INNER, OUTER, RIGHT_OUTER, LEFT_OUTER, CROSS_JOIN, SELF_JOIN};
	// there is no reason I cannot split this into 2 different classes other than laziness
	String joinType = null;
	String selector = null;
	String qualifier = null;
	GenRowStruct values = null;
	int count = 0;
	
	public Join(String lCol, String comparator, String rCol)
	{
		selector = lCol;
		qualifier = rCol;
		joinType = comparator;
	}
	
	public Join(String lCol, String comparator, GenRowStruct qualifiers)
	{
		selector = lCol;
		values = qualifiers;
		joinType = comparator;
	}

	
	public void setJoinType(String joinType)
	{
		this.joinType = joinType;
	}
	
	public String getSelector()
	{
		return selector;
	}
	
	public String getQualifier()
	{
		return qualifier;
	}
	
	public GenRowStruct getValues()
	{
		return values;
	}
	
	public void setValues(GenRowStruct values)
	{
		this.values = values;
	}
	
	
	public String getJoinType()
	{
		return this.joinType;
	}
}
