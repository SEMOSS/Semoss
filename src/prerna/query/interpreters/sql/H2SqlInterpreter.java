package prerna.query.interpreters.sql;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class H2SqlInterpreter extends NoOuterJoinSqlInterpreter {

	public H2SqlInterpreter() {
		
	}

	public H2SqlInterpreter(IEngine engine) {
		super(engine);
	}
	
	public H2SqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	//////////////////////////////////////append group by  ////////////////////////////////////////////
	
	@Override
	public StringBuilder appendGroupBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
		List<QueryColumnSelector> groupBy = ((SelectQueryStruct) this.qs).getGroupBy();
		int numGroups = groupBy.size();

		StringBuilder groupByName = new StringBuilder();;
		if(this.outerJoinsRequested) {
			boolean first = true;
			for(int i = 0; i < numGroups; i++) {
				QueryColumnSelector groupBySelector = groupBy.get(i);
				String colAlias = groupBySelector.getAlias();

				//if the groupBy selector is not among the user-requested selectors, then 
				//cannot be used as a groupBy selector
				if (selectorAliases.contains(colAlias)){
					if(!first) {
						groupByName.append(", ");
					} else {
						first = false;
					}
					groupByName.append(colAlias);
				} else {
					continue;
				}
			}
		} else {
			for(int i = 0; i < numGroups; i++) {
				QueryColumnSelector groupBySelector = groupBy.get(i);
				String tableConceptualName = groupBySelector.getTable();
				String columnConceptualName = groupBySelector.getColumn();
				
				if(this.customFromAliasName != null && !this.customFromAliasName.isEmpty()) {
					columnConceptualName = queryUtil.escapeReferencedAlias(columnConceptualName);
				} else {
					if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
						columnConceptualName = getPrimKey4Table(tableConceptualName);
					} else {
						columnConceptualName = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
					}
				}
				
				String groupByTable = getAlias(tableConceptualName);
				String groupByColumn = columnConceptualName;
				if(queryUtil.isSelectorKeyword(groupByTable)) {
					groupByTable = queryUtil.getEscapeKeyword(groupByTable);
				}
				if(queryUtil.isSelectorKeyword(groupByColumn)) {
					groupByColumn = queryUtil.getEscapeKeyword(groupByColumn);
				}
				
				if(i > 0) {
					groupByName.append(", ");
				}
				
				groupByName.append(groupByTable).append(".").append(groupByColumn);
			}
		}
		
		if(numGroups > 0 && groupByName.length() > 0) {
			query.append(" GROUP BY ").append(groupByName);
		}
		return query;
	}
	
}
