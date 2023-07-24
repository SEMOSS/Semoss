package prerna.query.interpreters.sql;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabase;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;

public class HiveSqlInterpreter extends SqlInterpreter {

	public HiveSqlInterpreter() {

	}

	public HiveSqlInterpreter(IDatabase engine) {
		super(engine);
	}

	public HiveSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}


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

		String customFrom = this.qs.getCustomFrom();
		this.customFromAliasName = this.qs.getCustomFromAliasName();

		addJoins();
		addSelectors();
		addFilters();
		addHavingFilters();
		addOrderBys();
		addOrderBySelector();
		
		StringBuilder query = new StringBuilder("SELECT ");
		String distinct = "";
		if(((SelectQueryStruct) this.qs).isDistinct()) {
			distinct = "DISTINCT ";
		}

		if(customFrom != null && !customFrom.isEmpty()) {
			// at the moment
			// no join logic with custom from
			query.append(distinct).append(selectors).append(" FROM (").append(customFrom).append(" ) AS " + this.customFromAliasName);
		} else {
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
			boolean appendStartingFrom = true;
			if(this.joinStructList.isEmpty() || this.joinStructList.allSubqueryJoins()) {
				appendStartingFrom = false;
				query.append(" FROM ");
				if(this.froms.isEmpty() && this.frame != null) {
					query.append(frame.getName());
				} else {
					String[] startPoint = this.froms.get(0);
					query.append(startPoint[0]).append(" ").append(startPoint[1]).append(" ");
				}
			} 
			if(!this.joinStructList.isEmpty()) {
				query.append(" ").append(joinStructList.getJoinSyntax(appendStartingFrom));
			}
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

		query = appendOrderBy(query);

		long limit = ((SelectQueryStruct) this.qs).getLimit();
		long offset = ((SelectQueryStruct) this.qs).getOffset();

		query = this.queryUtil.addLimitOffsetToQuery(query, limit, offset);

		if(logger.isDebugEnabled()) {
			if(query.length() > 500) {
				logger.debug("HIVE QUERY....  " + query.substring(0,  500) + "...");
			} else {
				logger.debug("HIVE QUERY....  " + query);
			}
		}
		
		return query.toString();
	}
	@Override
	public void addSelectors() {
		List<IQuerySelector> selectorData = qs.getSelectors();
		for(IQuerySelector selector : selectorData) {
			addSelector(selector);
		}
	}

	@Override
	protected void addSelector(IQuerySelector selector) {
		if(((SelectQueryStruct) this.qs).isDistinct()) {
			if(selector.getSelectorType()==IQuerySelector.SELECTOR_TYPE.FUNCTION){
				((SelectQueryStruct) this.qs).setDistinct(false);
			}
		}
		String alias = selector.getAlias();
		String newSelector = processSelector(selector, true) + " AS " + alias;
		if(selectors.length() == 0) {
			selectors = newSelector;
		} else {
			selectors += " , " + newSelector;
		}
		selectorList.add(newSelector);
		selectorAliases.add(alias);
	}
	
	@Override
	protected void addOrderBySelector() {
		int counter = 0;
		for(StringBuilder orderBySelector : this.orderBySelectors) {
			String alias = "o"+counter++;
			String newSelector = "("+orderBySelector+") AS " + "\""+alias+"\"";
			if(selectors.length() == 0) {
				selectors = newSelector;
			} else {
				selectors += " , " + newSelector;
			}
			selectorList.add(newSelector);
			selectorAliases.add(alias);
		}
	}
}
