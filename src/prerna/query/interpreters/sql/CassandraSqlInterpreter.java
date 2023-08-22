package prerna.query.interpreters.sql;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.util.Constants;

public class CassandraSqlInterpreter extends SqlInterpreter {


	public CassandraSqlInterpreter() {

	}

	public CassandraSqlInterpreter(IDatabaseEngine engine) {
		super(engine);
	}

	public CassandraSqlInterpreter(ITableDataFrame frame) {
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

		String customFrom = this.qs.getCustomFrom();
		this.customFromAliasName = this.qs.getCustomFromAliasName();
		// we do the joins since when we get to adding the from portion of the query
		// we want to make sure that table is not used within the joins
		addJoins();
		addSelectors();
		addFilters();
		addHavingFilters();
		addOrderBys();
		addOrderBySelector();
		
		//other wise us the standard logic
		if(this.qs.getBigDataEngine()){
			//big data engine is never distinct
			((SelectQueryStruct) this.qs).setDistinct(false);

			Boolean taskOptionsExist = false;
			Object isTaskProp = this.qs.getPragmap().get(Constants.TASK_OPTIONS_EXIST);
			if(isTaskProp != null){
				taskOptionsExist= Boolean.parseBoolean(isTaskProp.toString());
			}

			// if task options exists, it likely a vis
			if(!taskOptionsExist){
				// if there is 1 selector, group by it instead
				if (((SelectQueryStruct) this.qs).getSelectors().size() == 1){
					IQuerySelector selector = ((SelectQueryStruct) this.qs).getSelectors().get(0);
					if(selector.getSelectorType()==IQuerySelector.SELECTOR_TYPE.COLUMN){
						((SelectQueryStruct) this.qs).addGroupBy((QueryColumnSelector)selector);
					} 
				}
			}
		}

		StringBuilder query = new StringBuilder("SELECT ");
		String distinct = "";
		if(((SelectQueryStruct) this.qs).isDistinct()) {
			distinct = "DISTINCT ";
		}

		// do we have a custom from?
		if(customFrom != null && !customFrom.isEmpty()) {
			// at the moment
			// no join logic with custom from
			query.append(distinct).append(selectors).append(" FROM (").append(customFrom).append(" ) AS " + this.customFromAliasName);
		} else {
			// logic for adding the selectors + the from statement + the joins
			query.append(distinct).append(selectors);
			
			// if there is a join
			// can only have one table in from in general sql case 
			// thus, the order matters 
			// so get a good starting from table
			// we can use any of the froms that is not part of the join
			List<String> startPoints = new Vector<>();
			if(joinStructList.isEmpty()) {
				query.append(" FROM ");
				String[] startPoint = froms.get(0);
				query.append(startPoint[0]).append(" ").append(startPoint[1]).append(" ");
				startPoints.add(startPoint[1]);
			} else {
				query.append(" ").append(joinStructList.getJoinSyntax(true));
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
		
	
		logger.info("SQL QUERY....  " + query);

		return query.toString();
	}
	
}
