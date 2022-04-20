package prerna.query.querystruct.joins;

public class BasicRelationship implements IRelation {

	private String fromConcept;
	private String joinType;
	private String toConcept;
	private String comparator;
	private String relationName;
	
	public BasicRelationship() {
		
	}
	
	// fromConcept, joinType, toConcept, comparator, relName
	public BasicRelationship(String[] joinDetails) {
		this.fromConcept = joinDetails[0];
		this.joinType = joinDetails[1];
		this.toConcept = joinDetails[2];
		if(joinDetails.length > 3) {
			this.comparator = joinDetails[3];
		}
		if(joinDetails.length > 4) {
			this.relationName = joinDetails[4];
		}
	}
	
	public BasicRelationship(String fromConcept, String joinType, String toConcept, String comparator, String relationName) {
		this.fromConcept = fromConcept;
		this.joinType = joinType;
		this.toConcept = toConcept;
		this.comparator = comparator;
		this.relationName = relationName;
	}

	public String getFromConcept() {
		return fromConcept;
	}

	public void setFromConcept(String fromConcept) {
		this.fromConcept = fromConcept;
	}

	public String getJoinType() {
		return joinType;
	}

	public void setJoinType(String joinType) {
		this.joinType = joinType;
	}

	public String getToConcept() {
		return toConcept;
	}

	public void setToConcept(String toConcept) {
		this.toConcept = toConcept;
	}

	public String getRelationName() {
		return relationName;
	}

	public void setRelationName(String relationName) {
		this.relationName = relationName;
	}
	
	public String getComparator() {
		return comparator;
	}

	public void setComparator(String comparator) {
		this.comparator = comparator;
	}

	@Override
	public RELATION_TYPE getRelationType() {
		return RELATION_TYPE.BASIC;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(obj == null) {
			return false;
		}
		if(obj instanceof BasicRelationship) {
			BasicRelationship otherRel = (BasicRelationship) obj;
			if(this.fromConcept.equals(otherRel.fromConcept)
					&& this.joinType.equals(otherRel.joinType)
					&& this.toConcept.equals(otherRel.toConcept)
					) {
				return true;
			}
		}
		return false;
	}
}
