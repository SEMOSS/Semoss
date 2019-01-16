package prerna.query.interpreters.sql;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.sql.SQLQueryUtil;

public class H2SqlInterpreter extends NoOuterJoinSqlInterpreter {

	public H2SqlInterpreter() {
		
	}

	public H2SqlInterpreter(IEngine engine) {
		this.engine = engine;
		queryUtil = SQLQueryUtil.initialize(((RDBMSNativeEngine) engine).getDbType());
	}
	
	public H2SqlInterpreter(ITableDataFrame frame) {
		this.frame = frame;
	}
	
	@Override
	public StringBuilder appendOrderBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
		List<QueryColumnOrderBySelector> orderBy = ((SelectQueryStruct) this.qs).getOrderBy();
		List<StringBuilder> validOrderBys = new Vector<StringBuilder>();
		
		if(this.outerJoinsRequested) {
			for(QueryColumnOrderBySelector orderBySelector : orderBy) {
				String colAlias = orderBySelector.getAlias();
				ORDER_BY_DIRECTION orderByDir = orderBySelector.getSortDir();

				StringBuilder thisOrderBy = new StringBuilder();
				if (selectorAliases.contains(colAlias)){
					thisOrderBy.append(colAlias);
				} else {
					continue;
				}
				
				if(orderByDir == ORDER_BY_DIRECTION.ASC) {
					thisOrderBy.append(" ASC ");
				} else {
					thisOrderBy.append(" DESC ");
				}
				validOrderBys.add(thisOrderBy);
			}
		} else {
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
				else if(this.retTableToCols.containsKey(tableConceptualName)){
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
	
	//////////////////////////////////////append group by  ////////////////////////////////////////////
	
	@Override
	public StringBuilder appendGroupBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
		List<QueryColumnSelector> groupBy = ((SelectQueryStruct) this.qs).getGroupBy();

		String groupByName = null;
		if(this.outerJoinsRequested) {
			for(QueryColumnSelector groupBySelector : groupBy) {
				String colAlias = groupBySelector.getAlias();

				//if the groupBy selector is not among the user-requested selectors, then 
				//cannot be used as a groupBy selector
				if (selectorAliases.contains(colAlias)){
					if(groupByName == null) {
						groupByName = colAlias;
					} else {
						groupByName += ", "+ colAlias;
					}
				} else {
					continue;
				}
			}
		} else {
			for(QueryColumnSelector groupBySelector : groupBy) {
				String tableConceptualName = groupBySelector.getTable();
				String columnConceptualName = groupBySelector.getColumn();

				if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
					columnConceptualName = getPrimKey4Table(tableConceptualName);
				} else {
					columnConceptualName = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
				}

				if(groupByName == null) {
					groupByName = getAlias(tableConceptualName) + "." + columnConceptualName;
				} else {
					groupByName += ", "+ getAlias(tableConceptualName) + "." + columnConceptualName;
				}	
			}
		}
		
		if(groupByName != null) {
			query.append(" GROUP BY ").append(groupByName);
		}
		return query;
	}
	
}
