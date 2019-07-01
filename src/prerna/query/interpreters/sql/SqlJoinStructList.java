package prerna.query.interpreters.sql;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.stream.Collectors;

import prerna.util.Utility;

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
	 * Get a subset of the joinstructlist
	 * @param startIndex (inclusive)
	 * @param endIndex (inclusive)
	 * @return
	 */
	public SqlJoinStructList getSubsetJoinStructList(int startIndex, int endIndex) {
		SqlJoinStructList joinStructSubset = new SqlJoinStructList();
		for (int i = startIndex; i <= endIndex; i++) {
			joinStructSubset.addJoin(joins.get(i));
		}
		
		return joinStructSubset;
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
		int numJoins = joins.size();
		StringBuilder jSyntax = new StringBuilder();
		jSyntax.append("FROM ");
		
		Set<String> definedTables = new HashSet<String>();
		for(int i = 0; i < numJoins; i++) {
			// get the joins in the order they were defined
			SqlJoinStruct j = joins.get(i);
			String sourceTable = j.getSourceTable();
			String sourceTableAlias = j.getSourceTableAlias();
			String sourceCol = j.getSourceCol();
			
			String targetTable = j.getTargetTable();
			String targetTableAlias = j.getTargetTableAlias();
			String targetCol = j.getTargetCol();
			
			String jType = j.getJoinType();
			
			if(i == 0) {
				// we gotta define the first from
				jSyntax.append(sourceTable).append(" ").append(sourceTableAlias);
				jSyntax.append(" ").append(jType).append(" ");
				jSyntax.append(targetTable).append(" ").append(targetTableAlias);

				jSyntax.append(" on ")
				.append(sourceTableAlias).append(".").append(sourceCol)
				.append("=")
				.append(targetTableAlias).append(".").append(targetCol);
				
				definedTables.add(sourceTable);
				definedTables.add(targetTable);
				
			} else {
				// the join order matters
				// so the next join needs to have at least one of its tables
				// already defined
				// either the source or the target
				if(!definedTables.contains(sourceTable)) {
					// need to define the source
					// if the source is not defined
					jSyntax.append(" ").append(jType).append(" ");
					jSyntax.append(sourceTable).append(" ").append(sourceTableAlias);

					// add source table since it is now defined
					definedTables.add(sourceTable);
				} else if(!definedTables.contains(targetTable)) {
					// need to define the target
					jSyntax.append(" ").append(jType).append(" ");
					jSyntax.append(targetTable).append(" ").append(targetTableAlias);

					// add target table as it is now defined
					definedTables.add(targetTable);
				} 
//				else {
//					// both are defined
//					// need to make a new alias for the table
//					// at this point, i am not using this to bring in new values
//					// but to filter 
//					jSyntax.append(" ").append(jType).append(" ");
//					targetTableAlias = targetTableAlias + Utility.getRandomString(6);
//					jSyntax.append(targetTable).append(" ").append(targetTableAlias);
//				}
				
				// define the rest of the join portion
				jSyntax.append(" on ")
				.append(sourceTableAlias).append(".").append(sourceCol)
				.append("=")
				.append(targetTableAlias).append(".").append(targetCol);
			}
		}
		
		return jSyntax.toString();
	}
	
	public String getJoinSyntax(String derivedTableName, Set<String> traversedTables, Map<String, LinkedHashSet<String>> retTableToSelectors) {
		int numJoins = joins.size();
		StringBuilder jSyntax = new StringBuilder();

		Set<String> definedTables = new HashSet<String>();
		for(int i = 0; i < numJoins; i++) {
			// get the joins in the order they were defined
			SqlJoinStruct j = joins.get(i);
			String sourceTable = j.getSourceTable();
			String sourceTableAlias = j.getSourceTableAlias();
			String sourceCol = j.getSourceCol();
			
			String targetTable = j.getTargetTable();
			String targetTableAlias = j.getTargetTableAlias();
			String targetCol = j.getTargetCol();
			
			String jType = j.getJoinType();
			
			if(i == 0 && traversedTables.isEmpty()) {
				// we gotta define the first from
				jSyntax.append(sourceTable).append(" ").append(sourceTableAlias);
				jSyntax.append(" ").append(jType).append(" ");
				jSyntax.append(targetTable).append(" ").append(targetTableAlias);

				jSyntax.append(" on ")
				.append(sourceTableAlias).append(".").append(sourceCol)
				.append("=")
				.append(targetTableAlias).append(".").append(targetCol);
				
				definedTables.add(sourceTable);
				definedTables.add(targetTable);
			} else {
				// the join order matters
				// so the next join needs to have at least one of its tables
				// already defined
				// either the source or the target
				if((traversedTables.contains(sourceTable)) || (!definedTables.isEmpty() && definedTables.contains(sourceTable))) {
					// need to define the target
					jSyntax.append(" ").append(jType).append(" ");
					jSyntax.append(targetTable).append(" ").append(targetTableAlias);
					definedTables.add(targetTable);
				} else {				
					// need to define the source
					// if the source is not defined
					// i need to reverse the join type
					// if it is right to left
					// or left to right
					jSyntax.append(" ").append(j.getReverseJoinType()).append(" ");
					jSyntax.append(sourceTable).append(" ").append(sourceTableAlias);
					definedTables.add(sourceTable);
				}
				// if the source table is in the processedTables and not in the definedTables, 
				// then need to update the sourceTableAlias to the derivedTableName
				if (!traversedTables.isEmpty() && 
						traversedTables.contains(sourceTable) && !definedTables.contains(sourceTable)){
					String origSourceTableAlias = sourceTableAlias;
					String origSourceCol = sourceCol;
					// for the source col, need to find it via the table.colalias reference since that's how
					// it's stored in the derived table
					String sourceColSelector = retTableToSelectors.get(origSourceTableAlias).stream()
							.filter(s -> s.startsWith(origSourceTableAlias + "." + origSourceCol)).collect(Collectors.joining(""));
					sourceCol = sourceColSelector.split("\"")[1];
					sourceTableAlias = derivedTableName;
				}
				// define the rest of the join portion
				jSyntax.append(" on ")
				.append(sourceTableAlias).append(".").append(sourceCol)
				.append("=")
				.append(targetTableAlias).append(".").append(targetCol);
			}
		}
		
		return jSyntax.toString();
	}
	
	public String[] getOuterJoinSyntax(String derivedTableName, Set<String> traversedTables, 
			Map<String, LinkedHashSet<String>> retTableToSelectors, int jIndex){
		// string @ index 0 will be the left join syntax & string @ index 1 will be the right join syntax
		String[] outerJSyntax = new String[2];
		StringBuilder jSyntax = new StringBuilder();
		
		// get the joins in the order they were defined
		SqlJoinStruct j = joins.get(jIndex);
		String sourceTable = j.getSourceTable();
		String sourceTableAlias = j.getSourceTableAlias();
		String sourceCol = j.getSourceCol();
		
		String targetTable = j.getTargetTable();
		String targetTableAlias = j.getTargetTableAlias();
		String targetCol = j.getTargetCol();
		String jType = "left outer join";
		
		if(traversedTables.isEmpty()) {
			// we gotta define the first from
			jSyntax.append(sourceTable).append(" ").append(sourceTableAlias);
			jSyntax.append(" ").append(jType).append(" ");
			jSyntax.append(targetTable).append(" ").append(targetTableAlias);

			jSyntax.append(" on ")
			.append(sourceTableAlias).append(".").append(sourceCol)
			.append("=")
			.append(targetTableAlias).append(".").append(targetCol);
		} else {
			// check if the joinstruct needs to be reversed
			// if source is not in the processedTables list then need to reverse 
			if (!traversedTables.contains(sourceTable)) {
				j.reverse();
			}
			jSyntax.append(" ").append(jType).append(" ");
			jSyntax.append(j.getTargetTable()).append(" ").append(j.getTargetTableAlias());
			// define the rest of the join portion
			// for the source col, need to find it via the table.colalias reference since that's how
			// it's stored in the derived table
			String sourceColSelector = retTableToSelectors.get(j.getSourceTableAlias()).stream()
				.filter(i -> i.startsWith(j.getSourceTableAlias() + "." + j.getSourceCol())).collect(Collectors.joining(""));
			sourceCol = sourceColSelector.split("\"")[1];
			jSyntax.append(" on ")
			.append(derivedTableName).append(".").append(sourceCol)
			.append("=")
			.append(j.getTargetTableAlias()).append(".").append(j.getTargetCol());
		}
		
		outerJSyntax[0] = jSyntax.toString();
		outerJSyntax[1] = jSyntax.toString().replace("left outer join", "right outer join");

		return outerJSyntax;
	}
	
	public String getIndividualJoinSyntax() {
		StringBuilder jIndividualSyntax = new StringBuilder();
		
		
		return jIndividualSyntax.toString();
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
		
//		List<String[]> froms = jList.determineFromTables();
//		for(String[] f : froms) {
//			System.out.println(Arrays.toString(f));
//		}
		
		System.out.println(jList.getJoinSyntax());
	}
	
	
}
