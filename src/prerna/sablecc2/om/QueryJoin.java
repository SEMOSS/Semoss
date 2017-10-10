//package prerna.sablecc2.om;
//
//import prerna.ds.QueryStruct;
//import prerna.query.interpreters.QueryStructSelector;
//
//public class QueryJoin {
//
//	// NOT USED!!!
//
//	
//	/*
//	 * Define a join between 2 selectors
//	 * Syntax is : Join(upstream, comparator, downstream)
//	 */
//	
//	// left hand side of the join
//	private QueryStructSelector upstreamSelector;
//	// right hand side of the join
//	private QueryStructSelector downstreamSelector;
//	private String joinType;
//	private String relationshipName;
//	
//	public QueryJoin() {
//		
//	}
//	
//	public void setOptionsFromJoin(Join joinObject) {
//		this.upstreamSelector = new QueryStructSelector();
//		this.downstreamSelector = new QueryStructSelector();
//
//		String leftSelector = joinObject.getSelector();
//		if(leftSelector.contains("__")) {
//			String[] split = leftSelector.split("__");
//			this.upstreamSelector.setTable(split[0]);
//			this.upstreamSelector.setColumn(split[1]);
//		} else {
//			this.upstreamSelector.setTable(leftSelector);
//			this.upstreamSelector.setColumn(QueryStruct.PRIM_KEY_PLACEHOLDER);
//		}
//		
//		String rightSelector = joinObject.getQualifier();
//		if(rightSelector.contains("__")) {
//			String[] split = rightSelector.split("__");
//			this.downstreamSelector.setTable(split[0]);
//			this.downstreamSelector.setColumn(split[1]);
//		} else {
//			this.downstreamSelector.setTable(rightSelector);
//			this.downstreamSelector.setColumn(QueryStruct.PRIM_KEY_PLACEHOLDER);
//		}
//		
//		this.joinType = joinObject.getJoinType();
//		this.relationshipName = joinObject.getJoinRelName();
//	}
//	
//	
//	///////////////////////////////////////////
//	///////// Getters and Setters /////////////
//	///////////////////////////////////////////
//
//	public QueryStructSelector getUpstreamSelector() {
//		return upstreamSelector;
//	}
//
//	public void setUpstreamSelector(QueryStructSelector upstreamSelector) {
//		this.upstreamSelector = upstreamSelector;
//	}
//
//	public QueryStructSelector getDownstreamSelector() {
//		return downstreamSelector;
//	}
//
//	public void setDownstreamSelector(QueryStructSelector downstreamSelector) {
//		this.downstreamSelector = downstreamSelector;
//	}
//
//	public String getJoinType() {
//		return joinType;
//	}
//
//	public void setJoinType(String joinType) {
//		this.joinType = joinType;
//	}
//	
//	public String getRelationshipName() {
//		return relationshipName;
//	}
//
//	public void setRelationshipName(String relationshipName) {
//		this.relationshipName = relationshipName;
//	}
//	
//}
