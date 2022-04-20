package prerna.query.querystruct.joins;

import prerna.query.querystruct.SelectQueryStruct;

public class SubqueryRelationship implements IRelation {

	private SelectQueryStruct qs;
	private String queryAlias;
	private String joinType;
	private String fromConcept;
	private String toConcept;
	private String comparator;

	public SubqueryRelationship() {
		
	}
	
	public SubqueryRelationship(SelectQueryStruct qs, String queryAlias, String[] joinDetails) {
		this.qs = qs;
		this.queryAlias = queryAlias;
		this.fromConcept = joinDetails[0];
		this.joinType = joinDetails[1];
		this.toConcept = joinDetails[2];
		if(joinDetails.length > 3) {
			this.comparator = joinDetails[4];
		}
	}

	public SelectQueryStruct getQs() {
		return qs;
	}

	public void setQs(SelectQueryStruct qs) {
		this.qs = qs;
	}

	public String getQueryAlias() {
		return queryAlias;
	}

	public void setQueryAlias(String queryAlias) {
		this.queryAlias = queryAlias;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public String getFromConcept() {
		return fromConcept;
	}

	public void setFromConcept(String fromConcept) {
		this.fromConcept = fromConcept;
	}

	public String getToConcept() {
		return toConcept;
	}

	public void setToConcept(String toConcept) {
		this.toConcept = toConcept;
	}

	public String getComparator() {
		return comparator;
	}

	public void setComparator(String comparator) {
		this.comparator = comparator;
	}

	@Override
	public RELATION_TYPE getRelationType() {
		return RELATION_TYPE.SUBQUERY;
	}

}
