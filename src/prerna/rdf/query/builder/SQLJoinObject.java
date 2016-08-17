package prerna.rdf.query.builder;

import java.util.HashSet;
import java.util.Set;

public class SQLJoinObject {

	// contains the query string associated with this join object
	private StringBuilder queryString = new StringBuilder();
	
	// contains the list of values that are needed in order to determine 
	private Set<String> requiredTableDefinitions = new HashSet<String>(); 
	
	private Set<String> definedAliasWithinJoin = new HashSet<String>();
	
	// add the required information to get the appropriate query pertaining to the join
	public void addQueryString(String additionalJoinInfo) {
		if(queryString.length() == 0) {
			queryString.append(additionalJoinInfo);
		} else {
			queryString.append(" AND ").append(additionalJoinInfo);
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
	
	public String getQueryString() {
		return queryString.toString();
	}
	
	public Set<String> getDefinedAliasWithinJoin() {
		return this.definedAliasWithinJoin;
	}
}
