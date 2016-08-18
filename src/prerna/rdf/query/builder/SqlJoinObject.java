package prerna.rdf.query.builder;

import java.util.HashSet;
import java.util.Set;

public class SqlJoinObject {

	public enum SqlJoinTypeEnum {inner, left, right, outer, cross};
	
	// unique id for easy identification of the join object
	private String id;
	
	// contains the query string associated with this join object
	private StringBuilder queryString = new StringBuilder();
	
	// contains the list of values that are needed in order to determine 
	private Set<String> requiredTableDefinitions = new HashSet<String>(); 
	
	// contains the list of alias defined within the join object
	private Set<String> definedAliasWithinJoin = new HashSet<String>();
	
	// contains the join type
	private SqlJoinTypeEnum joinType;
	
	public SqlJoinObject(String id) {
		this.id = id;
	}
	
	// add the required information to get the appropriate query pertaining to the join
	public void addQueryString(String additionalJoinInfo) {
		if(queryString.length() == 0) {
			queryString.append(additionalJoinInfo);
		} else {
			queryString.append(" INNER JOIN ").append(additionalJoinInfo);
		}
	}
	
	// add required table alias definitions to be used by the join
	public void addTableAliasRequired(String tableAlias) {
		requiredTableDefinitions.add(tableAlias);
	}
	
	// add the table alias that this join string defines
	public void addTableAliasDefinedByJoin(String tableAlias) {
		definedAliasWithinJoin.add(tableAlias);
	}
	
	// see if aliases are defined such that when the join query portion
	// is added it will be valid
	public boolean tableDefinitionsMet(Set<String> definedAliasSet) {
		return definedAliasSet.containsAll(requiredTableDefinitions);
	}
	
	// get the query string for this join
	public String getQueryString() {
		return queryString.toString();
	}
	
	// get the defined alias for the tables within this join
	public Set<String> getDefinedAliasWithinJoin() {
		return this.definedAliasWithinJoin;
	}
	
	// set the join type
	public void setSqlJoinType(SqlJoinTypeEnum joinType) {
		this.joinType = joinType;
	}
	
	// get the join type
	public SqlJoinTypeEnum getSqlJoinType() {
		return this.joinType;
	}
	
	// get the join unique id
	public String getId() {
		return this.id;
	}
	
	@Override
	public String toString() {
		return this.id;
	}
	
	@Override
	public boolean equals(Object object) {
		// did the user pass in the id?
		if(object instanceof String) {
			if(object.toString().equals(this.id)) {
				return true;
			}
		} else if(object instanceof SqlJoinObject) {
			if(this == object) {
				return true;
			}
			
			if(this.queryString.toString().equals(((SqlJoinObject) object).queryString.toString())) {
				return true;
			}
		}
		
		return false;
	}
}
