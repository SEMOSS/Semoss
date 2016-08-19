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
	
	// since we assume that the sql joins define a specific path
	// lets find which table is not defined inside the join
	// this will determine which table should be defined in the froms clause
	public String[] getTableNotDefinedInJoinList() {
		int joinIdx1 = 0;
		int numJoins = joinList.size();
		
		// store the table not defined in any of the joins
		String[] tableNotInJoin = null;
		
		FOUND_TABLE_LOOP : for(; joinIdx1 < numJoins; joinIdx1++) {
			// grab the first join
			SqlJoinObject join1 = joinList.get(joinIdx1);
			
			// get the required tables from this join
			// that means it needs to be defined before this join can be called
			List<String[]> requiredTables = join1.getAllRequiredTables();
			
			TABLE_LOOP : for(String[] reqTable : requiredTables) {
				String alias = reqTable[1];
				
				// now we need to test each required tables to all the existing joins
				
				int joinIdx2 = 0;
				for(; joinIdx2 < numJoins; joinIdx2++) {
					if(joinIdx1 == joinIdx2) {
						continue;
					}
					// grab the second join
					SqlJoinObject join2 = joinList.get(joinIdx2);
					
					// if this table is defined within another join
					// it ain't good
					// so continue the table loop
					if(join2.getDefinedAliasWithinJoin().contains(alias)) {
						continue TABLE_LOOP;
					}
				}
				
				// if we get to this point, we did not enter the continue table loop
				// while going through all of the other joins
				// thus, this table is not defined
				tableNotInJoin = reqTable;
				break FOUND_TABLE_LOOP;
			}
		}
		
		return tableNotInJoin;
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
	
	public boolean isEmpty() {
		return this.joinList.isEmpty();
	}

}
