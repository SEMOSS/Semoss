package prerna.query.interpreters.sql;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;

public class PostgresSqlInterpreter extends SqlInterpreter {

	public PostgresSqlInterpreter() {
		
	}

	public PostgresSqlInterpreter(IDatabaseEngine engine) {
		super(engine);
	}
	
	public PostgresSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	/*
	 * Same as parent but replacing "outer join" with "full outer join"
	 */
	@Override
	protected void addJoin(String fromCol, String thisComparator, String toCol, String comparator) {
		// get the parts of the join
		List<String[]> relConPropList = getRelationshipConceptProperties(fromCol, toCol);
		for(String[] relConProp : relConPropList) {
			String sourceTable = relConProp[0];
			String sourceColumn = relConProp[1];
			String targetTable = relConProp[2];
			String targetColumn = relConProp[3];
			
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
			// set the comparator
			jStruct.setComparator(comparator);
			
			joinStructList.addJoin(jStruct);
		}
	}

}
