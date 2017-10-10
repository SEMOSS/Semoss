package prerna.sablecc2.om;

// left column
// right column
// type of join
public class Join {

	//public enum JOIN_TYPE {INNER, OUTER, RIGHT_OUTER, LEFT_OUTER, CROSS_JOIN, SELF_JOIN};
	// there is no reason I cannot split this into 2 different classes other than laziness --it has been split
	private String joinType = null;
	private String selector = null;
	private String qualifier = null;
	private String relName = null;
	int count = 0;
	
	public Join(String lCol, String joinType, String rCol)
	{
		this.selector = lCol;
		this.qualifier = rCol;
		this.joinType = joinType;
	}

	
	public Join(String lCol, String joinType, String rCol, String relName) 
	{
		this.selector = lCol;
		this.qualifier = rCol;
		this.joinType = joinType;
		this.relName = relName;
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
		
	public String getJoinType()
	{
		return this.joinType;
	}
	
	public String getJoinRelName() 
	{
		return this.relName;
	}
}
