package prerna.rdf.query.builder;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class SqlJoinObject {

	public enum SqlJoinTypeEnum {inner, left, right, outer, cross};
	
	// unique id for easy identification of the join object
	private String id;
	
	// contains the query string associated with this join object
	private StringBuilder queryString = new StringBuilder();
	
	// contains the list of values that are needed in order to determine 
	private Map<String, String> requiredTableDefinitions = new Hashtable<String, String>();
	
	// contains the list of alias defined within the join object
	private Map<String, String> definedAliasWithinJoin = new Hashtable<String, String>();
	
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
//			if(SqlJoinTypeEnum.inner == joinType) {
				queryString.append(" ").append(additionalJoinInfo);
//			} else if(SqlJoinTypeEnum.left == joinType) {
//				queryString.append(" left outer join ").append(additionalJoinInfo);
//			} else if(SqlJoinTypeEnum.right == joinType) {
//				queryString.append(" right outer join ").append(additionalJoinInfo);
//			} else if(SqlJoinTypeEnum.outer == joinType) {
//				queryString.append(" outer join ").append(additionalJoinInfo);
//			} else if(SqlJoinTypeEnum.cross == joinType) {
//				queryString.append(" cross join ").append(additionalJoinInfo);
//			} else {
//				queryString.append(" inner join ").append(additionalJoinInfo);
//			}
		}
	}
	
	public void addQueryString(String additionalJoinInfo, String compName) {
//		if(queryString.length() == 0) {
//			queryString.append(" ").append(compName).append(" ").append(additionalJoinInfo);
//		} else {
//			if(SqlJoinTypeEnum.inner == joinType) {
				queryString.append(" ").append(compName).append(" ").append(additionalJoinInfo);
//			} else if(SqlJoinTypeEnum.left == joinType) {
//				queryString.append(" left outer join ").append(additionalJoinInfo);
//			} else if(SqlJoinTypeEnum.right == joinType) {
//				queryString.append(" right outer join ").append(additionalJoinInfo);
//			} else if(SqlJoinTypeEnum.outer == joinType) {
//				queryString.append(" outer join ").append(additionalJoinInfo);
//			} else if(SqlJoinTypeEnum.cross == joinType) {
//				queryString.append(" cross join ").append(additionalJoinInfo);
//			} else {
//				queryString.append(" inner join ").append(additionalJoinInfo);
//			}
//		}
	}
	
	// add required table alias definitions to be used by the join
	public void addTableAliasRequired(String tableAlias, String tableName) {
		requiredTableDefinitions.put(tableAlias, tableName);
	}
	
	// add the table alias that this join string defines
	public void addTableAliasDefinedByJoin(String tableAlias, String tableName) {
		definedAliasWithinJoin.put(tableAlias, tableName);
	}
	
	// see if aliases are defined such that when the join query portion
	// is added it will be valid
	public boolean tableDefinitionsMet(Set<String> definedAliasSet) {
		return definedAliasSet.containsAll(requiredTableDefinitions.keySet());
	}
	
	// get the query string for this join
	public String getQueryString() {
		return queryString.toString();
	}
	
	public int getNumRequiredTables() {
		return requiredTableDefinitions.keySet().size();
	}
	
	// get a list of all the required tables for the join
	// list contains a String[] of tableName and alias
	public List<String[]> getAllRequiredTables() {
		List<String[]> results = new Vector<String[]>();
		// iterate through and create list of required tables and their aliases
		for(String alias : requiredTableDefinitions.keySet()) {
			String[] reqEntry = new String[]{requiredTableDefinitions.get(alias), alias};
			results.add(reqEntry);
		}
		
		return results;
	}
	
	// get the defined alias for the tables within this join
	public Set<String> getDefinedAliasWithinJoin() {
		return this.definedAliasWithinJoin.keySet();
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
