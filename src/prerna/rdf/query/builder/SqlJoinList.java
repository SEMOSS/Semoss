package prerna.rdf.query.builder;

import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class SqlJoinList {

	// store the list of sql joins within the query
	private List<SqlJoinObject> joinList = new Vector<SqlJoinObject>();

	// store a map of join ids to indices in joinList
	private Map<String, Integer> joinPositionMap = new Hashtable<String, Integer>();


	public void addSqlJoinObject(SqlJoinObject join) {
		this.joinList.add(join);
		this.joinPositionMap.put(join.getId(), this.joinList.size()-1);
	}

	// determine if all joins are inner
	// sql query has shortcuts when this is the case
	public boolean allInnerJoins() {
		int joinIdx = 0;
		int size = joinList.size();
		for(; joinIdx < size; joinIdx++) {
			if(joinList.get(joinIdx).getSqlJoinType() != SqlJoinObject.SqlJoinTypeEnum.inner) {
				return false;
			}
		}

		return true;
	}

	// does join already exist
	public boolean doesJoinAlreadyExist(String joinId) {
		return joinPositionMap.containsKey(joinId);
	}

	// get existing join object
	public SqlJoinObject getExistingJoin(String joinId) {
		if(joinPositionMap.containsKey(joinId)) {
			return joinList.get(joinPositionMap.get(joinId));
		}
		return null;
	}
	
	// determine if appropriate path exists
	public String getJoinPath(String startingTableAlias) throws RuntimeException{
		StringBuilder query = new StringBuilder();
		// keep track of all the joins that we have already added
		Set<String> usedJoins = new HashSet<String>();
		// keep track of the set of available alias we have defined
		// note that we start with the set defined in the from portion of the query
		Set<String> availableAlias = new HashSet<String>();
		availableAlias.add(startingTableAlias);
		// keep a counter showing the number of joins we have added and the total number
		// so we know when we are done
		int joinCounter = 0;
		int totalJoinsToAdd = joinList.size();

		//TODO: this is a very greedy way of doing this
		//TODO: should figure out a smart way to do a path problem...

		// we loop through until every join is added
		while(joinCounter < totalJoinsToAdd) {
			// we keep track of a boolean that must be modified with each iteration
			// if it is not, we know the join information does not produce a valid path
			// and we know that query will not work
			boolean pathHasBeenModified = false;
			
			for(int joinIdx = 0; joinIdx < totalJoinsToAdd; joinIdx++) {
				SqlJoinObject joinObject = joinList.get(joinIdx);
				// so we dont add the same joins twice
				if(!usedJoins.contains(joinObject.getId())) {
					// do we have all the necessary information to add the join
					if(joinObject.tableDefinitionsMet(availableAlias)) {
						// add the query from the join object
						query.append(" ").append(joinObject.getQueryString());
						// update the used joins so we dont try to add again
						usedJoins.add(joinObject.getId());
						// update the available aliases
						availableAlias.addAll(joinObject.getDefinedAliasWithinJoin());
						// update the counter so the while loop can end
						joinCounter++;
						
						pathHasBeenModified = true;
					}
				}
			}
			
			if(!pathHasBeenModified) {
				throw new IllegalArgumentException("Join Information in query is not a valid path.");
			}
		}
		
		return query.toString();
	}

}
