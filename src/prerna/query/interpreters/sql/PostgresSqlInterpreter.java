package prerna.query.interpreters.sql;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;

public class PostgresSqlInterpreter extends SqlInterpreter {

	public PostgresSqlInterpreter() {
		
	}

	public PostgresSqlInterpreter(IEngine engine) {
		super(engine);
	}
	
	public PostgresSqlInterpreter(ITableDataFrame frame) {
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
		
		joinStructList.addJoin(jStruct);
	}
	
	@Override
	public StringBuilder appendOrderBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
		List<QueryColumnOrderBySelector> orderBy = ((SelectQueryStruct) this.qs).getOrderBy();
		List<StringBuilder> validOrderBys = new Vector<StringBuilder>();
		for(QueryColumnOrderBySelector orderBySelector : orderBy) {
			String tableConceptualName = orderBySelector.getTable();
			String columnConceptualName = orderBySelector.getColumn();
			ORDER_BY_DIRECTION orderByDir = orderBySelector.getSortDir();
			
			boolean origPrim = false;
			if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
				origPrim = true;
				columnConceptualName = getPrimKey4Table(tableConceptualName);
			} else {
				columnConceptualName = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
			}
			
			StringBuilder thisOrderBy = new StringBuilder();
			
			// might want to order by a derived column being returned
			if(origPrim && this.selectorAliases.contains(tableConceptualName)) {
				// either instantiate the string builder or add a comma for multi sort
				thisOrderBy.append("\"").append(tableConceptualName).append("\"");
			}
			// we need to make sure the sort is a valid one!
			// if it is not already processed, there is no way to sort it...
			else if(this.retTableToCols.containsKey(tableConceptualName)) {
				if(this.retTableToCols.get(tableConceptualName).contains(columnConceptualName)) {
					thisOrderBy.append(getAlias(tableConceptualName)).append(".").append(columnConceptualName);
				} else {
					continue;
				}
			} 
			
			// well, this is not a valid order by to add
			else {
				continue;
			}
			
			if(orderByDir == ORDER_BY_DIRECTION.ASC) {
				thisOrderBy.append(" ASC ");
			} else {
				thisOrderBy.append(" DESC ");
			}
			validOrderBys.add(thisOrderBy);
		}
		
		int size = validOrderBys.size();
		for(int i = 0; i < size; i++) {
			if(i == 0) {
				query.append(" ORDER BY ");
			} else {
				query.append(", ");
			}
			query.append(validOrderBys.get(i).toString());
		}
		return query;
	}
	
}
