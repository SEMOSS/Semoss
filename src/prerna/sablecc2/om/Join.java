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
	private String joinRelName = null;

	public Join(String lCol, String joinType, String rCol) {
		this.selector = lCol;
		this.qualifier = rCol;
		this.joinType = joinType;
	}


	public Join(String lCol, String joinType, String rCol, String joinRelName) {
		this.selector = lCol;
		this.qualifier = rCol;
		this.joinType = joinType;
		this.joinRelName = joinRelName;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public String getSelector() {
		return selector;
	}

	public void setSelector(String selector) {
		this.selector = selector;
	}

	public String getQualifier() {
		return qualifier;
	}

	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}

	public String getJoinRelName() {
		return joinRelName;
	}

	public void getJoinRelName(String joinRelName) {
		this.joinRelName = joinRelName;
	}

}
