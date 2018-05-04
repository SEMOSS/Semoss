package prerna.query.interpreters.sql;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

public class SqlJoinStructList {

	private List<SqlJoinStruct> joins = new Vector<SqlJoinStruct>();
	
	public SqlJoinStructList() {
		
	}
	
	public void addJoin(SqlJoinStruct join) {
		if(!joins.contains(join)) {
			joins.add(join);
		}
	}

	/**
	 * Get all the joins of a certain type
	 * @param jType
	 * @return
	 */
	public List<SqlJoinStruct> getJoinsOfType(String jType) {
		List<SqlJoinStruct> joinsOfType = new Vector<SqlJoinStruct>();
		for(SqlJoinStruct j : joins) {
			if(j.getJoinType().equals(jType)) {
				joinsOfType.add(j);
			}
		}
		return joinsOfType;
	}
	
	/**
	 * Get the from + join syntax
	 * @return
	 */
	public String getJoinSyntax() {
		// we will start by creating the from
		// and then traverse down
		// until all the joins have had the required tables defined
		// and we define them
		
		Set<String> definedAlias = new HashSet<String>();
		int numJoins = joins.size();
		Set<Integer> jIndexAdded = new HashSet<Integer>();
		
		StringBuilder jSyntax = new StringBuilder();
		jSyntax.append("FROM ");
		List<String[]> froms = determineFromTables();
		int numFroms = froms.size();
		if(numFroms == 0) {
			throw new IllegalArgumentException("Join Information in query is not a valid path.");
		} else if(numFroms > 1) {
			// we need to modify the joins
			// so that we only have 1 from
			froms = modfiyJoinsForSingleFrom(froms);
			numFroms = froms.size();
		}
		
		// THIS SHOULD BE SIZE 1
		// Not sure about all sql
		// but it is very likely this will not work
		for(int i = 0; i < numFroms; i++) {
			String[] f = froms.get(i);
			if(i != 0) {
				jSyntax.append(", ");
			}
			jSyntax.append(f[0]).append(" ").append(f[1]);
			
			// add the alias that we can now use
			definedAlias.add(f[1]);
		}
		
		// now we need to go through and find all the paths for the join
		// we need to make sure we have defined the appropriate tables
		// it may take a few iterations
		// to make sure we have gone through everything
		RESTART : while(jIndexAdded.size() < numJoins) {
			
			for(int i = 0; i < numJoins; i++) {
				if(jIndexAdded.contains(new Integer(i))) {
					continue;
				}
				
				SqlJoinStruct j = joins.get(i);
				
				// target is the required alias
				// that must be defined
				// we will define the source
				String targetTableAlias = j.getTargetTableAlias();
				if(definedAlias.contains(targetTableAlias)) {
					// we can add this guy
					
					String jType = j.getJoinType();
					// get source info
					String souceTable = j.getSourceTable();
					String sourceTableAlias = j.getSourceTableAlias();
					String sourceColumn = j.getSourceCol();
					// get target info
					// remember, the target table is already defined
					String targetColumn = j.getTargetCol();
					
					// add the inner join portion
					// define the source if we need to
					jSyntax.append(" ").append(jType).append(" ");
					if(!definedAlias.contains(sourceTableAlias)) {
						jSyntax.append(souceTable).append(" ").append(sourceTableAlias);
					}
					jSyntax.append(" on ")
						.append(sourceTableAlias).append(".").append(sourceColumn)
						.append("=")
						.append(targetTableAlias).append(".").append(targetColumn);
					
					// add the source as a new defined alias
					jIndexAdded.add(new Integer(i));
					definedAlias.add(sourceTableAlias);
					
					// we start the loop starting at the first index
					// since our otherJoin map always has smallest index to largest
					continue RESTART;
				}
			}
		}
		
		return jSyntax.toString();
	}
	

	private List<String[]> modfiyJoinsForSingleFrom(List<String[]> froms) {
		// we need to rearrange the joins
		// to account for a single from
		
		// we need to loop through again
		// and see number of times what alias are defined 
		// this is the set of source aliases we have
		int numJoins = joins.size();
		Map<String, Integer> definedAlias = new HashMap<String, Integer>();
		for(int i = 0; i < numJoins; i++) {
			SqlJoinStruct joinStruct = joins.get(i);
			
			String sourceAlias = joinStruct.getSourceTableAlias();
			// defined is source
			if(definedAlias.containsKey(sourceAlias)) {
				int curNum = definedAlias.get(sourceAlias);
				curNum++;
				definedAlias.put(sourceAlias, new Integer(curNum));
			} else {
				definedAlias.put(sourceAlias, new Integer(1));
			}
		}
		
		List<Integer> fromIndexRemoved = new Vector<Integer>();
		int fromCounter = 0;
		int numFroms = froms.size();
		// let us loop through and try to switch the joins
		RESTART : while(fromCounter < numFroms) {
			
			// break out of the loop once we have only 1 from
			if( (numFroms - fromIndexRemoved.size())  == 1) {
				break;
			}
			
			if(fromIndexRemoved.contains(new Integer(fromCounter))) {
				fromCounter++;
				continue;
			}
			
			// since we are modifying what is defined
			// and what is not
			// we need to continue going through
			String[] f = froms.get(fromCounter);
			String aliasNeedsDefining = f[1];
			for(int i = 0; i < numJoins; i++) {
				
				SqlJoinStruct j = joins.get(i);
				// we need to modify this join
				// if the from reflects the target that isn't defined
				String targetTableAlias = j.getTargetTableAlias();
				if(targetTableAlias.equals(aliasNeedsDefining)) {
					// is the source defined
					String sourceTableAlias = j.getSourceTableAlias();
					// do we have multiple things defining the source?
					int numDefined = definedAlias.get(sourceTableAlias);
					if(numDefined > 1) {
						// we can do a switch
						j.reverse();
						
						// now add it as a defined alias
						definedAlias.put(targetTableAlias, new Integer(1));
						
						// lower the number of the other one
						int curNum = definedAlias.get(sourceTableAlias);
						curNum--;
						definedAlias.put(sourceTableAlias, new Integer(curNum));
						
						// store the from to remove
						fromIndexRemoved.add(new Integer(fromCounter));
						
						// we also need to reset the loop
						fromCounter = 0;
						continue RESTART;
					}
				}
			}
			
			// if we get to this point
			// we need to try the next from
			fromCounter++;
		}
		
		List<String[]> singleFrom = new Vector<String[]>();
		for(int i = 0; i < numFroms; i++) {
			if(!fromIndexRemoved.contains(new Integer(i))) {
				singleFrom.add(froms.get(i));
			}
		}
		
		return singleFrom;
	}

	/**
	 * Determine the from tables that must be defined in the join syntax
	 * @return
	 */
	public List<String[]> determineFromTables() {
		Set<String> fromAlias = new HashSet<String>();
		List<String[]> fromTables = new Vector<String[]>();
		
		/*
		 * We go through each join
		 * When we go through the joins
		 * so for every join
		 * we define the source
		 * and need the target
		 * so we need to go through and see what is not defined
		 */

		int size = joins.size();
		if(size == 1) {
			SqlJoinStruct joinStruct = joins.get(0);
			fromTables.add(new String[]{joinStruct.getTargetTable(), joinStruct.getTargetTableAlias()});
		} else {
			Set<String> requiredAlias = new HashSet<String>();
			Set<String> definedAlias = new HashSet<String>();
			for(int i = 0; i < size; i++) {
				SqlJoinStruct joinStruct = joins.get(i);
				
				String sourceAlias = joinStruct.getSourceTableAlias();
				String targetAlias = joinStruct.getTargetTableAlias();
				
				// again, required is target
				// defined is source
				requiredAlias.add(targetAlias);
				definedAlias.add(sourceAlias);
			}
			
			// now that i have all the required and defined
			// remove all the required that are defined
			// so required alias is really the alias that still need
			// to be defined
			requiredAlias.removeAll(definedAlias);
	
			// now loop through the required alias list
			// and add the joins that we need
			for(int i = 0; i < size; i++) {
				SqlJoinStruct joinStruct = joins.get(i);
				
				String targetTable = joinStruct.getTargetTable();
				String targetTableAlias = joinStruct.getTargetTableAlias();
				
				if(!fromAlias.contains(targetTableAlias) && requiredAlias.contains(targetTableAlias)) {
					fromTables.add(new String[]{targetTable, targetTableAlias});
					fromAlias.add(targetTableAlias);
				}
			}
		}
			
		return fromTables;
	}
	
	public boolean isEmpty() {
		return this.joins.isEmpty();
	}
	
	public void clear() {
		this.joins.clear();
	}
	
	
	public static void main(String[] args) {
		
		SqlJoinStructList jList = new SqlJoinStructList();
		
		/*
			FROM offices offices  
			inner  join employees employees ON offices.officeCode = employees.officeCode 
			inner  join customers customers ON employees.employeeNumber = customers.salesRepEmployeeNumber 
			inner  join orders orders ON customers.customerNumber = orders.customerNumber 
			inner  join payments payments ON customers.customerNumber = payments.customerNumber LIMIT 100
		 */
		
		SqlJoinStruct j0 = new SqlJoinStruct();
		j0.setJoinType("inner join");
		j0.setSourceTable("customers");
		j0.setSourceCol("customerNumber");
		j0.setTargetTable("payments");
		j0.setTargetCol("customerNumber");
		jList.addJoin(j0);
		
		SqlJoinStruct j1 = new SqlJoinStruct();
		j1.setJoinType("inner join");
		j1.setSourceTable("offices");
		j1.setSourceCol("officeCode");
		j1.setTargetTable("employees");
		j1.setTargetCol("officeCode");
		jList.addJoin(j1);
		
		SqlJoinStruct j2 = new SqlJoinStruct();
		j2.setJoinType("inner join");
		j2.setSourceTable("employees");
		j2.setSourceCol("employeeNumber");
		j2.setTargetTable("customers");
		j2.setTargetCol("salesRepEmployeeNumber");
		jList.addJoin(j2);
		
		SqlJoinStruct j3 = new SqlJoinStruct();
		j3.setJoinType("inner join");
		j3.setSourceTable("customers");
		j3.setSourceCol("customerNumber");
		j3.setTargetTable("orders");
		j3.setTargetCol("customerNumber");
		jList.addJoin(j3);
		
		List<String[]> froms = jList.determineFromTables();
		for(String[] f : froms) {
			System.out.println(Arrays.toString(f));
		}
		
		System.out.println(jList.getJoinSyntax());
	}
	
	
}
