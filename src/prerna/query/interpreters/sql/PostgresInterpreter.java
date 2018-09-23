package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;

public class PostgresInterpreter extends SqlInterpreter {

	public PostgresInterpreter() {
		
	}

	public PostgresInterpreter(IEngine engine) {
		super(engine);
	}
	
	public PostgresInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	/**
	 * Adds the join to the relationHash which gets added to the query in composeQuery
	 * @param fromCol					The starting column, this can be just a table
	 * 									or table__column
	 * @param thisComparator			The comparator for the type of join
	 * @param toCol						The ending column, this can be just a table
	 * 									or table__column
	 */
	protected void addJoin(String fromCol, String thisComparator, String toCol) {
		// get the parts of the join
		String[] relConProp = getRelationshipConceptProperties(fromCol, toCol);
		String targetTable = relConProp[0];
		String targetColumn = relConProp[1];
		String sourceTable = relConProp[2];
		String sourceColumn = relConProp[3];
		
		String compName = thisComparator.replace(".", " ");
		SqlJoinStruct jStruct = new SqlJoinStruct();
		// POSTGRES sql syntax requires the 'full' in outer join
		compName = compName.trim();
		if(compName.equals("outer join")) {
			compName = "full outer join";
		}
		jStruct.setJoinType(compName);
		// add source
		jStruct.setSourceTable(sourceTable);
		jStruct.setSourceTableAlias(getAlias(sourceTable));
		jStruct.setSourceCol(sourceColumn);
		// add target
		jStruct.setTargetTable(targetTable);
		jStruct.setTargetTableAlias(getAlias(targetTable));
		jStruct.setTargetCol(targetColumn);
		
		joinStructList.addJoin(jStruct);
	}
	
}
