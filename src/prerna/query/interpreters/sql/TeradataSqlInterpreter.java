package prerna.query.interpreters.sql;

import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactor;
import prerna.util.sql.TeradataQueryUtil;

public class TeradataSqlInterpreter  extends SqlInterpreter {

	
	public TeradataSqlInterpreter() {
		
	}

	public TeradataSqlInterpreter(IEngine engine) {
		super(engine);
		}
	
	public TeradataSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	//overriden since i need limit before the order by
	@Override
	public String composeQuery()
	{
		if(this.qs instanceof HardSelectQueryStruct) {
			return ((HardSelectQueryStruct)this.qs).getQuery();
		}
		/*
		 * Need to create the query... 
		 * This to consider:
		 * 1) the user is going to be using the conceptual names as defined by the OWL (if present
		 * and OWL is the improved version). This has a few consequences:
		 * 1.a) when a user enters a table name, we need to determine what the primary key is
		 * 		for that table
		 * 1.b) need to consider what tables are used within joins and which are not. this will
		 * 		determine when we add it to the from clause or if the table will be defined via 
		 * 		the join 
		 */

		// we do the joins since when we get to adding the from portion of the query
		// we want to make sure that table is not used within the joins
		addJoins();
		addSelectors();
		addFilters();
		addHavingFilters();

		StringBuilder query = new StringBuilder("SELECT ");
		String distinct = "";
		if(((SelectQueryStruct) this.qs).isDistinct()) {
			distinct = "DISTINCT ";
		}
		if(this.engine != null && !engine.isBasic() && joinStructList.isEmpty()) {
			// if there are no joins, we know we are querying from a single table
			// the vast majority of the time, there shouldn't be any duplicates if
			// we are selecting all the columns
			String table = froms.get(0)[0];
			if(engine != null && !engine.isBasic()) {
				String physicalUri = engine.getPhysicalUriFromPixelSelector(table);
				if( (engine.getPhysicalConcepts().size() == 1) && (engine.getPropertyUris4PhysicalUri(physicalUri).size() + 1) == selectorList.size()) {
					// plus one is for the concept itself
					// no distinct needed
					query.append(selectors);
				} else {
					query.append(distinct).append(selectors);
				}
			} else {
				// need a distinct
				query.append(distinct).append(selectors).append(" FROM ");
			}
		} else {
			// default is to use a distinct
			query.append(distinct).append(selectors);
		}
		
		// if there is a join
		// can only have one table in from in general sql case 
		// thus, the order matters 
		// so get a good starting from table
		// we can use any of the froms that is not part of the join
		List<String> startPoints = new Vector<String>();
		if(joinStructList.isEmpty()) {
			query.append(" FROM ");
			String[] startPoint = froms.get(0);
			query.append(startPoint[0]).append(" ").append(startPoint[1]).append(" ");
			startPoints.add(startPoint[1]);
		} else {
			query.append(" ").append(joinStructList.getJoinSyntax());
		}
		
		// add where clause filters
		int numFilters = this.filterStatements.size();
		for(int i = 0; i < numFilters; i++) {
			if(i == 0) {
				query.append(" WHERE ");
			} else {
				query.append(" AND ");
			}
			query.append(this.filterStatements.get(i).toString());
		}
		
		//grab the order by and get the corresponding display name for that order by column
		query = appendGroupBy(query);
		// add having filters
		numFilters = this.havingFilterStatements.size();
		for(int i = 0; i < numFilters; i++) {
			if(i == 0) {
				query.append(" HAVING ");
			} else {
				query.append(" AND ");
			}
			query.append(this.havingFilterStatements.get(i).toString());
		}

		
		long limit = ((SelectQueryStruct) this.qs).getLimit();
		long offset = ((SelectQueryStruct) this.qs).getOffset();
		
		String tempTable = Utility.getRandomString(6);

		query = ((TeradataQueryUtil) this.queryUtil).addLimitOffsetToQuery(query, limit, offset, tempTable);
		
		query = appendOrderBy(query, tempTable);

		if(query.length() > 500) {
			logger.debug("SQL QUERY....  " + query.substring(0,  500) + "...");
		} else {
			logger.debug("SQL QUERY....  " + query);
		}

		return query.toString();
	}

	private StringBuilder appendOrderBy(StringBuilder query, String tempTable) {
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
						if(queryUtil.isSelectorKeyword(tableConceptualName)) {
							thisOrderBy.append(queryUtil.getEscapeKeyword(tempTable));
						} else {
							thisOrderBy.append(tempTable);
						}
					}
					// we need to make sure the sort is a valid one!
					// if it is not already processed, there is no way to sort it...
					else if(this.retTableToCols.containsKey(tableConceptualName)){
						if(this.retTableToCols.get(tableConceptualName).contains(columnConceptualName)) {
							String orderByColumn = columnConceptualName;
							if(queryUtil.isSelectorKeyword(orderByColumn)) {
								orderByColumn = queryUtil.getEscapeKeyword(orderByColumn);
							}
							thisOrderBy.append(tempTable).append(".").append(orderByColumn);
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
