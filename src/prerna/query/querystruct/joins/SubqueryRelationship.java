package prerna.query.querystruct.joins;

import java.util.ArrayList;
import java.util.List;

import prerna.query.querystruct.SelectQueryStruct;

public class SubqueryRelationship implements IRelation {

	private SelectQueryStruct qs;
	private String queryAlias;
	private String joinType;
	private List<String[]> joinOnDetails = new ArrayList<>();

	public SubqueryRelationship() {
		
	}
	
	public SubqueryRelationship(SelectQueryStruct qs, String queryAlias, String joinType, String[] joinOnDetails) {
		this.qs = qs;
		this.queryAlias = queryAlias;
		this.joinType = joinType;
		this.joinOnDetails.add(joinOnDetails);
	}
	
	public SubqueryRelationship(SelectQueryStruct qs, String queryAlias, String joinType, List<String[]> joinDetails) {
		this.qs = qs;
		this.queryAlias = queryAlias;
		this.joinType = joinType;
		this.joinOnDetails = joinDetails;
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

	public void setJoinOnDetails(List<String[]> joinOnDetails) {
		this.joinOnDetails = joinOnDetails;
	}

	public void addJoinOn(String[] joinOn) {
		this.joinOnDetails.add(joinOn);
	}
	
	public void addJoinOn(String fromConcept, String toConcept, String comparator) {
		if(comparator == null) {
			comparator = "=";
		}
		this.joinOnDetails.add(new String[] {fromConcept, toConcept, comparator});
	}
	
	public List<String[]> getJoinOnDetails() {
		return this.joinOnDetails;
	}

	@Override
	public RELATION_TYPE getRelationType() {
		return RELATION_TYPE.SUBQUERY;
	}

}
