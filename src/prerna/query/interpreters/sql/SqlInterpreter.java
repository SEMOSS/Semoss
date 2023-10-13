package prerna.query.interpreters.sql;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.interpreters.AbstractQueryInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.BetweenQueryFilter;
import prerna.query.querystruct.filters.FunctionQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.joins.BasicRelationship;
import prerna.query.querystruct.joins.IRelation;
import prerna.query.querystruct.joins.SubqueryRelationship;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySort;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.reactor.IReactor;
import prerna.reactor.qs.SubQueryExpression;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryCustomOrderBy;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryIfSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.om.task.ITask;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SqlQueryUtilFactory;

public class SqlInterpreter extends AbstractQueryInterpreter {

	private static final Logger classLogger = LogManager.getLogger(SqlInterpreter.class);

	// this keeps the table aliases
	protected HashMap<String, String> aliases = new HashMap<>();
	
	// keep track of processed tables used to ensure we don't re-add tables into the from string
	protected HashMap<String, String> tablesProcessed = new HashMap<>();
	
	// we will keep track of the conceptual names to physical names so we don't re-query the owl multiple times
	protected transient Map<String, String> conceptualConceptToPhysicalMap = new HashMap<>();
	// need to also keep track of the properties
	protected transient Map<String, String> conceptualPropertyToPhysicalMap = new HashMap<>();
	// need to keep track of the primary key for tables
	protected transient Map<String, String> primaryKeyCache = new HashMap<>();

	// we can create a statement without an engine... 
	// but everything needs to be the physical schema
	protected transient IDatabaseEngine engine; 
	protected transient ITableDataFrame frame;
	protected AbstractSqlQueryUtil queryUtil = SqlQueryUtilFactory.initialize(RdbmsTypeEnum.H2_DB);

	// where the wheres are all kept
	// key is always a combination of concept and comparator
	// and the values are values
	protected List<String> filterStatements = new ArrayList<>();
	protected List<String> havingFilterStatements = new ArrayList<>();
	
	protected transient Map<String, List<String[]>> relationshipConceptPropertiesMap = new HashMap<>();
	
	protected String selectors = "";
	protected Set<String> selectorList = new HashSet<>();
	// keep selector alias
	protected List<String> selectorAliases = new ArrayList<>();
	// keep list of columns for tables
	protected Map<String, List<String>> retTableToCols = new HashMap<>();
	
	protected String customFromAliasName = null;
	protected List<String[]> froms = new ArrayList<>();
	// store the joins in the object for easy use
	protected SqlJoinStructList joinStructList = new SqlJoinStructList();
	protected List<String> subQsAliasNames = new ArrayList<>();
	
	// if we have distinct
	// then custom order bys must be added in
	protected List<StringBuilder> orderBys = new ArrayList<>();
	protected List<StringBuilder> orderBySelectors = new ArrayList<>();
	
	public SqlInterpreter() {
		
	}

	public SqlInterpreter(IDatabaseEngine engine) {
		this.engine = engine;
		this.queryUtil = ((IRDBMSEngine) engine).getQueryUtil();
	}
	
	public SqlInterpreter(ITableDataFrame frame) {
		this.frame = frame;
	}

	/**
	 * Main method to invoke to take the QueryStruct to compose the appropriate SQL query
	 */
	@Override
	public String composeQuery()
	{
		if(this.qs instanceof HardSelectQueryStruct) {
			return ((HardSelectQueryStruct)this.qs).getQuery();
		}
		// if this is gen expression
		// compose query and send it
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
			boolean appendStartingFrom = true;
			if(this.joinStructList.isEmpty()) {
				appendStartingFrom = false;
				query.append(" FROM ");
				if(this.froms.isEmpty() && this.frame != null) {
					query.append(frame.getName());
				} else {
					String[] startPoint = this.froms.get(0);
					query.append(startPoint[0]).append(" ").append(startPoint[1]).append(" ");
				}
			} else {
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
				logger.debug("SQL QUERY....  " + query.substring(0,  500) + "...");
			} else {
				logger.debug("SQL QUERY....  " + query);
			}
		}
		
		return query.toString();
	}

	//////////////////////////// adding selectors //////////////////////////////////////////
	
	/**
	 * Loops through the selectors defined in the QS to add them to the selector string
	 * and considers if the table should be added to the from string
	 */
	public void addSelectors() {
		List<IQuerySelector> selectorData = qs.getSelectors();
		for(IQuerySelector selector : selectorData) {
			addSelector(selector);
		}
	}
	
	protected void addSelector(IQuerySelector selector) {
		String alias = selector.getAlias();
		String newSelector = processSelector(selector, true) + " AS " + "\""+alias+"\"";
		if(selectors.length() == 0) {
			selectors = newSelector;
		} else {
			selectors += " , " + newSelector;
		}
		selectorList.add(newSelector);
		selectorAliases.add(alias);
	}
	
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
	
	/**
	 * Method is used to generate the appropriate syntax for each type of selector
	 * Note, this returns everything without the alias since this is called again from
	 * the base methods it calls to allow for complex math expressions
	 * @param selector
	 * @return
	 */
	protected String processSelector(IQuerySelector selector, boolean addProcessedColumn) {
		IQuerySelector.SELECTOR_TYPE selectorType = selector.getSelectorType();
		if(selectorType == IQuerySelector.SELECTOR_TYPE.CONSTANT) {
			return processConstantSelector((QueryConstantSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.COLUMN) {
			return processColumnSelector((QueryColumnSelector) selector, addProcessedColumn);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.FUNCTION) { 
			return processFunctionSelector((QueryFunctionSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.ARITHMETIC) {
			return processArithmeticSelector((QueryArithmeticSelector) selector);
		} else if(selectorType == IQuerySelector.SELECTOR_TYPE.OPAQUE) {
			return processOpaqueSelector((QueryOpaqueSelector) selector);
		}else if(selectorType == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
			return processIfElseSelector((QueryIfSelector) selector, addProcessedColumn, false);
		}
		return null;
	}
	
	private String processIfElseSelector(QueryIfSelector selector, boolean addProcessedColumn, boolean anotherCondition)
	{
		// get the condition first
		IQueryFilter condition = selector.getCondition();
		StringBuffer buf = null;
		if(anotherCondition) {
			buf = new StringBuffer("WHEN ");
		} else {
			buf = new StringBuffer("CASE WHEN ");
		}
		
		StringBuilder filterBuilder = new StringBuilder();
		filterBuilder = this.processFilter(condition);

		// builder shoudl have what we need at this point
		buf.append(filterBuilder.toString());
		buf.append("  THEN  ");
		
		// get the precedent
		IQuerySelector precedent = selector.getPrecedent();
		if(precedent.getSelectorType() == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
			// note - this is a full case when that is embedded and has its own CASE WHEN + END
			buf.append(" ( ").append(processIfElseSelector((QueryIfSelector) precedent, addProcessedColumn, false))
				.append(" ) ");
		} else {
			buf.append(processSelector(precedent, addProcessedColumn));
		}

		IQuerySelector antecedent = selector.getAntecedent();
		if(antecedent != null) {
			// if the antecedent is another if reactor.. we need to pull this and then continue
			// if queryifselector - then start with another case
			// otherwise go with else
			if(antecedent instanceof QueryIfSelector) {
				buf.append(" ");
				buf.append(processIfElseSelector((QueryIfSelector) antecedent, addProcessedColumn, true));
			} else {
				buf.append(" ELSE ");
				buf.append(processSelector(antecedent, addProcessedColumn));
			}
		}
		// only add the end once 
		if(!anotherCondition) {
			buf.append(" END ");
		}
		
		return buf.toString();
	}


	protected String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof SubQueryExpression) {
			ITask innerTask = null;
			try {
				innerTask = ((SubQueryExpression) constant).generateQsTask();
				innerTask.setLogger(logger);
				if(innerTask.hasNext()) {
					Object value = innerTask.next().getValues()[0];
					if(value instanceof Number) {
						return value.toString();
					} else {
						return "'" + AbstractSqlQueryUtil.escapeForSQLStatement(constant + "") + "'";
					}
				}
			} catch(Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
			} finally {
				if(innerTask != null) {
					try {
						innerTask.close();
					} catch (IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			// if this doesn't return anything...
			return "NULL";
		} else if(constant instanceof Number) {
			return constant.toString();
		} else if(constant instanceof Boolean){ 
			if(queryUtil.allowBooleanDataType()) {
				return Boolean.parseBoolean(constant + "") + "";
			} else {
				// append 1 or 0 based on true/false
				if(Boolean.parseBoolean(constant + "")) {
					return "CAST(1 as " + queryUtil.getBooleanDataTypeName() + ")";
				} else {
					return "CAST(0 as " + queryUtil.getBooleanDataTypeName() + ")";
				}
			}
		} else {
			return "'" + AbstractSqlQueryUtil.escapeForSQLStatement(constant + "") + "'";
		}
	}

	/**
	 * The second
	 * @param selector
	 * @param isTrueColumn
	 * @return
	 */
	protected String processColumnSelector(QueryColumnSelector selector, boolean notEmbeddedColumn) {
		String table = selector.getTable();
		String colName = selector.getColumn();
		String tableAlias = selector.getTableAlias();
		String physicalColName = null;

		if(this.subQsAliasNames.contains(table)) {
			// this is a column selector from a projection off a subquery
			tableAlias = table;
			physicalColName = colName;
			if(this.queryUtil != null) {
				physicalColName = this.queryUtil.escapeSubqueryColumnName(physicalColName);
			}
		} else {
			if(tableAlias == null) {
				if(this.customFromAliasName != null && !this.customFromAliasName.isEmpty()) {
					tableAlias = this.customFromAliasName;
				} else {
					tableAlias = getAlias(getPhysicalTableNameFromConceptualName(table));
				}
			}
			// account for keywords
			if(queryUtil.isSelectorKeyword(tableAlias)) {
				tableAlias = queryUtil.getEscapeKeyword(tableAlias);
			}
			
			if(this.customFromAliasName != null) {
				// the column is not on a table
				// but on the custom from
				physicalColName = queryUtil.escapeReferencedAlias(colName);
			} else {
				// will be getting the physical column name
				physicalColName = colName;
				// if engine is not null, get the info from the engine
				if(engine != null && !engine.isBasic()) {
					// if the colName is the primary key placeholder
					// we will go ahead and grab the primary key from the table
					if(colName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
						physicalColName = getPrimKey4Table(table);
						// the display name is defaulted to the table name
					} else {
						// default assumption is the info being passed is the conceptual name
						// get the physical from the conceptual
						physicalColName = getPhysicalPropertyNameFromConceptualName(table, colName);
					}
				}
			}
			
			// need to perform this check 
			// if there are no joins
			// or all the joins are from a subquery
			// we need to have a from table
			if(this.joinStructList.isEmpty()) {
				addFrom(table, tableAlias);
			}
		}
		// keep track of all the processed columns
		if(notEmbeddedColumn) {
			this.retTableToCols.putIfAbsent(table, new ArrayList<String>());
			this.retTableToCols.get(table).add(physicalColName);
		}
		
		// if its an illegal char
		// first,  you are a jerk for how you made your table
		// but we will escape it
		if(queryUtil.isSelectorKeyword(physicalColName)) {
			physicalColName = queryUtil.getEscapeKeyword(physicalColName);
		}
		
		return tableAlias + "." + physicalColName;
	}
	
	protected String processFunctionSelector(QueryFunctionSelector selector) {
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		
		String function = selector.getFunction();
		
		StringBuilder expression = new StringBuilder();
		expression.append(this.queryUtil.getSqlFunctionSyntax(function)).append("(");
		if(selector.isDistinct()) {
			expression.append("DISTINCT ");
		}

		int size = innerSelectors.size();	
		for(int i = 0; i< size; i++) {
			if(i == 0) {
				expression.append(processSelector(innerSelectors.get(i), false));
			} else {
				expression.append(",").append(processSelector(innerSelectors.get(i), false));
			}
		}
		
		if(function.equalsIgnoreCase(QueryFunctionHelper.CAST)){
			String dataType = selector.getDataType();
			expression.append(" AS " + dataType);
		}
		
		// add any default function options as defined by the query util
		this.queryUtil.appendDefaultFunctionOptions(selector);
		List<Object[]> additionalParams = selector.getAdditionalFunctionParams();
		for(int i = 0; i < additionalParams.size(); i++) {
			expression.append(",");
			Object[] param = additionalParams.get(i);
			String name = param[0].toString();
			if(!name.equals("noname")) {
				expression.append(name).append(" ");
			}
			for(int j = 1; j < param.length; j++) {
				if(j > 1) {
					expression.append(",");
				}
				expression.append(param[j]);
			}
		}
		expression.append(")");
		
		return expression.toString();
	}
	
	protected String processArithmeticSelector(QueryArithmeticSelector selector) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		
		if(mathExpr.equals("/")) {
			return "( CAST(" + processSelector(leftSelector, false) + " AS DECIMAL) " + mathExpr + " CAST(NULLIF(" + processSelector(rightSelector, false) + ",0) AS DECIMAL) )";
		} else {
			return "( CAST(" + processSelector(leftSelector, false) + "  AS DECIMAL) " + mathExpr + " CAST(" + processSelector(rightSelector, false) + " AS DECIMAL) )";
		}
	}
	
	protected String processOpaqueSelector(QueryOpaqueSelector selector) {
		if(this.joinStructList.isEmpty() && selector.getTable() != null) {
			addFrom(selector.getTable(), selector.getTable());
		}
		return selector.getQuerySelectorSyntax();
	}
	
	//////////////////////////////////// end adding selectors /////////////////////////////////////
	
	
	/////////////////////////////////// adding from ////////////////////////////////////////////////
	
	
	/**
	 * Adds the form statement for each table
	 * @param conceptualTableName			The name of the table
	 */
	protected void addFrom(String conceptualTableName, String alias) {
		// need to determine if we can have multiple froms or not
		// we don't want to add the from table multiple times as this is invalid in sql
		if(!tablesProcessed.containsKey(conceptualTableName)) {
			tablesProcessed.put(conceptualTableName, "true");
			
			// we want to use the physical table name
			String physicalTableName = getPhysicalTableNameFromConceptualName(conceptualTableName);
			
			if(queryUtil.isSelectorKeyword(physicalTableName)) {
				physicalTableName = queryUtil.getEscapeKeyword(physicalTableName);
			}
			froms.add(new String[]{physicalTableName, alias});
		}
	}

	////////////////////////////////////// end adding from ///////////////////////////////////////
	
	
	////////////////////////////////////// adding joins /////////////////////////////////////////////
	
	/**
	 * Adds the joins for the query
	 */
	public void addJoins() {
		if(this.queryUtil != null) {
			this.joinStructList.setQueryUtil(this.queryUtil);
		}
		for(IRelation relationship : qs.getRelations()) {
			if(relationship.getRelationType() == IRelation.RELATION_TYPE.BASIC) {
				BasicRelationship rel = (BasicRelationship) relationship;
				String from = rel.getFromConcept();
				String joinType = rel.getJoinType();
				String to = rel.getToConcept();
				String comparator = rel.getComparator();
				if(comparator == null) {
					comparator = "=";
				}
				addJoin(from, joinType, to, comparator);
			} else if(relationship.getRelationType() == IRelation.RELATION_TYPE.SUBQUERY){
				addSubqueryJoin((SubqueryRelationship) relationship);
			} else {
				logger.info("Cannot process relationship of type: " + relationship.getRelationType());
			}
		}
	}

	/**
	 * Adds the join to the relationHash which gets added to the query in composeQuery
	 * @param fromCol					The starting column, this can be just a table
	 * 									or table__column
	 * @param thisJoinType			The comparator for the type of join
	 * @param toCol						The ending column, this can be just a table
	 * 									or table__column
	 */
	protected void addJoin(String fromCol, String thisJoinType, String toCol, String comparator) {
		// get the parts of the join
		List<String[]> relConPropList = getRelationshipConceptProperties(fromCol, toCol);
		for(String[] relConProp : relConPropList) {
			String sourceTable = relConProp[0];
			String sourceColumn = relConProp[1];
			String targetTable = relConProp[2];
			String targetColumn = relConProp[3];
			
			String joinType = thisJoinType.replace(".", " ");
			SqlJoinStruct jStruct = new SqlJoinStruct();
			jStruct.setJoinType(joinType);
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
	
	protected void addSubqueryJoin(SubqueryRelationship rel) {
		SelectQueryStruct subQs = rel.getQs();
		String queryAlias = rel.getQueryAlias();
		String joinType = rel.getJoinType().replace(".", " ");
		List<String[]> jDetails = rel.getJoinOnDetails();

		SqlInterpreter innerInterpreter = null;
		try {
			innerInterpreter = this.getClass().newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		if (innerInterpreter == null) {
			throw new NullPointerException("innerInterpreter cannot be null here.");
		}
		// set the necessary references
		innerInterpreter.engine = this.engine;
		innerInterpreter.queryUtil = this.queryUtil;
		innerInterpreter.frame = this.frame;
		
		innerInterpreter.setQueryStruct(subQs);
		innerInterpreter.setLogger(this.logger);
		String innerQuery = innerInterpreter.composeQuery();
		
		SqlJoinStruct jStruct = new SqlJoinStruct();
		jStruct.setUseSubQuery(true);
		jStruct.setSubQuery(innerQuery);
		jStruct.setSubQueryAlias(queryAlias);
		jStruct.setJoinType(joinType);
		
		for(String[] jDetail : jDetails) {
			String fromTable = null;
			String fromColumn = null;
			String toTable = null;
			String toColumn = null;
			{
				final String fromConcept = jDetail[0];
				if(!fromConcept.contains("__")) {
					throw new IllegalArgumentException("Subquery Joins require join details in format TABLE__COLUMN");
				}
				String[] split = fromConcept.split("__");
				fromTable = split[0];
				fromColumn = split[1];
				if(engine != null && !engine.isBasic()) {
					if(!fromTable.equals(queryAlias)) {
						// if we already have it, just grab from hash
						if(conceptualConceptToPhysicalMap.containsKey(fromTable)) {
							fromTable = conceptualConceptToPhysicalMap.get(fromTable);
						} else {
							// we dont have it.. so query for it
							String physicalTableUri = this.engine.getPhysicalUriFromPixelSelector(fromTable);
							if(physicalTableUri != null) {
								// table name is the instance name of the URI
								String tableName = Utility.getInstanceName(physicalTableUri);
								// store the physical name as well in case we get it later
								conceptualConceptToPhysicalMap.put(fromTable, tableName);
								fromTable = tableName;
							}
						}
						
						if(conceptualPropertyToPhysicalMap.containsKey(fromConcept)) {
							fromColumn = conceptualPropertyToPhysicalMap.get(fromConcept);
						}
						// we don't have it... so query for it
						String colURI = this.engine.getPhysicalUriFromPixelSelector(fromConcept);
						if(colURI != null) {
							// the class is the name of the column
							String colName = Utility.getClassName(colURI);
							conceptualPropertyToPhysicalMap.put(fromConcept, colName);
							fromColumn = colName;
						}
					}
				}
			}
			{
				final String toConcept = jDetail[1];
				if(!toConcept.contains("__")) {
					throw new IllegalArgumentException("Subquery Joins require join details in format TABLE__COLUMN");
				}
				String[] split = toConcept.split("__");
				toTable = split[0];
				toColumn = split[1];
				
				if(engine != null && !engine.isBasic()) {
					if(!toTable.equals(queryAlias)) {
						// if we already have it, just grab from hash
						if(conceptualConceptToPhysicalMap.containsKey(toTable)) {
							toTable = conceptualConceptToPhysicalMap.get(toTable);
						} else {
							// we dont have it.. so query for it
							String physicalTableUri = this.engine.getPhysicalUriFromPixelSelector(toTable);
							if(physicalTableUri != null) {
								// table name is the instance name of the URI
								String tableName = Utility.getInstanceName(physicalTableUri);
								// store the physical name as well in case we get it later
								conceptualConceptToPhysicalMap.put(toTable, tableName);
								toTable = tableName;
							}
						}
						
						if(conceptualPropertyToPhysicalMap.containsKey(toConcept)) {
							toColumn = conceptualPropertyToPhysicalMap.get(toConcept);
						}
						// we don't have it... so query for it
						String colURI = this.engine.getPhysicalUriFromPixelSelector(toConcept);
						if(colURI != null) {
							// the class is the name of the column
							String colName = Utility.getClassName(colURI);
							conceptualPropertyToPhysicalMap.put(toConcept, colName);
							toColumn = colName;
						}
					}
				}
			}
			String comparator = jDetail[2];
			jStruct.addJoinOnList(new String[] {fromTable, fromColumn, toTable, toColumn, comparator});
		}
		
		joinStructList.addJoin(jStruct);
		
		// store the query alias
		// so we can reference this and not go to the OWL metadata
		subQsAliasNames.add(queryAlias);
	}
	
	////////////////////////////////////////// end adding joins ///////////////////////////////////////
	
	
	////////////////////////////////////////// adding filters ////////////////////////////////////////////
	
	public void addHavingFilters() {
		List<IQueryFilter> filters = qs.getHavingFilters().getFilters();
		for(IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter);
			if(filterSyntax != null) {
				this.havingFilterStatements.add(filterSyntax.toString());
			}
		}
	}
	
	public void addFilters() {
		List<IQueryFilter> filters = qs.getCombinedFilters().getFilters();
		for(IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter);
			if(filterSyntax != null) {
				this.filterStatements.add(filterSyntax.toString());
			}
		}
	}
	
	protected StringBuilder processFilter(IQueryFilter filter) {
		IQueryFilter.QUERY_FILTER_TYPE filterType = filter.getQueryFilterType();
		if(filterType == IQueryFilter.QUERY_FILTER_TYPE.SIMPLE) {
			return processSimpleQueryFilter((SimpleQueryFilter) filter);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.AND) {
			return processAndQueryFilter((AndQueryFilter) filter);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.OR) {
			return processOrQueryFilter((OrQueryFilter) filter);
		} else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.FUNCTION) {
			return processFunctionQueryFilter((FunctionQueryFilter) filter);
		}else if(filterType == IQueryFilter.QUERY_FILTER_TYPE.BETWEEN) {
			return processBetweenQueryFilter((BetweenQueryFilter) filter);
		}
		return null;
	}
	
	protected StringBuilder processOrQueryFilter(OrQueryFilter filter) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" OR ");
			}
			filterBuilder.append(processFilter(filterList.get(i)));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	protected StringBuilder processAndQueryFilter(AndQueryFilter filter) {
		StringBuilder filterBuilder = new StringBuilder();
		List<IQueryFilter> filterList = filter.getFilterList();
		int numAnds = filterList.size();
		for(int i = 0; i < numAnds; i++) {
			if(i == 0) {
				filterBuilder.append("(");
			} else {
				filterBuilder.append(" AND ");
			}
			filterBuilder.append(processFilter(filterList.get(i)));
		}
		filterBuilder.append(")");
		return filterBuilder;
	}

	protected StringBuilder processFunctionQueryFilter(FunctionQueryFilter filter) {
		QueryFunctionSelector functionSelector = filter.getFunctionSelector();
		List<IQuerySelector> innerSelectors = functionSelector.getInnerSelector();
		String function = functionSelector.getFunction();
		
		StringBuilder expression = new StringBuilder();
		expression.append(this.queryUtil.getSqlFunctionSyntax(function)).append("(");
		if(functionSelector.isDistinct()) {
			expression.append("DISTINCT ");
		}
		int size = innerSelectors.size();	
		for(int i = 0; i< size; i++) {
			if(i == 0) {
				expression.append(processSelector(innerSelectors.get(i), false));
			} else {
				expression.append(",").append(processSelector(innerSelectors.get(i), false));
			}
		}
		
		List<Object[]> additionalParams = functionSelector.getAdditionalFunctionParams();
		for(int i = 0; i < additionalParams.size(); i++) {
			expression.append(",");
			Object[] param = additionalParams.get(i);
			String name = param[0].toString();
			if(!name.equals("noname")) {
				expression.append(name).append(" ");
			}
			for(int j = 1; j < param.length; j++) {
				if(j > 1) {
					expression.append(",");
				}
				expression.append(param[j]);
			}
		}
		expression.append(")");
		return expression;
	}
	
	protected StringBuilder processBetweenQueryFilter(BetweenQueryFilter filter)
	{
		StringBuilder retBuilder = new StringBuilder();
		retBuilder.append(processSelector(filter.getColumn(), true));
		retBuilder.append("  BETWEEN  ");
		retBuilder.append(filter.getStart());
		retBuilder.append("  AND  ");
		retBuilder.append(filter.getEnd());
		return retBuilder;
	}
	
	protected StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getSimpleFilterType();
		if(fType == FILTER_TYPE.COL_TO_COL) {
			return addSelectorToSelectorFilter(leftComp, rightComp, thisComparator);
		} else if(fType == FILTER_TYPE.COL_TO_VALUES) {
			return addSelectorToValuesFilter(leftComp, rightComp, thisComparator);
		} else if(fType == FILTER_TYPE.VALUES_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			return addSelectorToValuesFilter(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator));
		} else if(fType == FILTER_TYPE.COL_TO_QUERY) {
			return addSelectorToQueryFilter(leftComp, rightComp, thisComparator);
		} else if(fType == FILTER_TYPE.QUERY_TO_COL) {
			return addSelectorToQueryFilter(leftComp, rightComp, IQueryFilter.getReverseNumericalComparator(thisComparator));
		} else if(fType == FILTER_TYPE.COL_TO_LAMBDA) {
			return addSelectorToLambda(leftComp, rightComp, thisComparator);
		} else if(fType == FILTER_TYPE.LAMBDA_TO_COL) {
			// same logic as above, just switch the order and reverse the comparator if it is numeric
			return addSelectorToLambda(rightComp, leftComp, IQueryFilter.getReverseNumericalComparator(thisComparator));
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
			return addValueToValueFilter(rightComp, leftComp, thisComparator);
		} 
		return null;
	}
	
	/**
	 * 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 * @return
	 */
	private StringBuilder addSelectorToLambda(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		// need to evaluate the lambda on the right
		IReactor reactor = (IReactor) rightComp.getValue();
		NounMetadata nounEvaluated = reactor.execute();

		Map<String, Object> mergeMetadata = reactor.mergeIntoQsMetadata();
		if(mergeMetadata.get(IReactor.MERGE_INTO_QS_FORMAT).equals(IReactor.MERGE_INTO_QS_FORMAT_SCALAR)) {
			return addSelectorToValuesFilter(leftComp, nounEvaluated, thisComparator);
		}
		
		throw new IllegalArgumentException("Unknown qs format to merge");
	}

	protected StringBuilder addSelectorToQueryFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftSelectorExpression = processSelector(leftSelector, false);
		
		SelectQueryStruct subQs = (SelectQueryStruct) rightComp.getValue();
		SqlInterpreter innerInterpreter = null;
		try {
			innerInterpreter = this.getClass().newInstance();
		} catch (InstantiationException | IllegalAccessException e) {
			logger.error(Constants.STACKTRACE, e);
		}
		if (innerInterpreter == null) {
			throw new NullPointerException("innerInterpreter cannot be null here.");
		}
		// set the necessary references
		innerInterpreter.engine = this.engine;
		innerInterpreter.queryUtil = this.queryUtil;
		innerInterpreter.frame = this.frame;
		
		innerInterpreter.setQueryStruct(subQs);
		innerInterpreter.setLogger(this.logger);
		String innerQuery = innerInterpreter.composeQuery();
		
		StringBuilder filterBuilder = new StringBuilder(leftSelectorExpression);
		if(thisComparator.trim().equals("==")) {
			filterBuilder .append(" IN ( ").append(innerQuery).append(" ) ");
		} else if(thisComparator.trim().equals("!=") || thisComparator.equals("<>")) {
			filterBuilder.append(" NOT IN ( ").append(innerQuery).append(" ) ");
		} else {
			filterBuilder.append(" ").append(thisComparator).append(" (").append(innerQuery).append(")");
		}
		return filterBuilder;
	}

	/**
	 * Add filter for a column to values
	 * @param filters 
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	protected StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		thisComparator = thisComparator.trim();
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftSelectorExpression = processSelector(leftSelector, false);
		String leftDataType = leftSelector.getDataType();
		
		// if it is null, then we know we have a column
		// need to grab from metadata
		if(leftDataType == null) {
			String leftConceptProperty = leftSelector.getQueryStructName();
			String[] leftConProp = getConceptProperty(leftConceptProperty);
			String leftConcept = leftConProp[0];
			String leftProperty = leftConProp[1];
	
			if(engine != null && !engine.isBasic()) {
				leftDataType = this.engine.getDataTypes("http://semoss.org/ontologies/Concept/" + leftProperty + "/" + leftConcept);
				// ugh, need to try if it is a property
				if(leftDataType == null) {
					leftDataType = this.engine.getDataTypes("http://semoss.org/ontologies/Relation/Contains/" + leftProperty + "/" + leftConcept);
				}
				if(leftDataType != null) {
					leftDataType = leftDataType.replace("TYPE:", "");
				}
			} else if(frame != null) {
				leftDataType = this.frame.getMetaData().getHeaderTypeAsString(leftConceptProperty);
			}
		}
		
		List<Object> objects = new ArrayList<>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof Collection) {
			objects.addAll( (Collection) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}
		
		// need to account for null inputs
		boolean addNullCheck = objects.remove(null);
		boolean nullCheckWithEquals = true;
		if(leftDataType != null && SemossDataType.isNotString(leftDataType)) {
			if(objects.remove("null")) {
				addNullCheck = true;
			}
			if(objects.remove("nan")) {
				addNullCheck = true;
			}
			if(thisComparator.equals("==") && objects.remove("")) {
				addNullCheck = true;
			}
		}
		if(!addNullCheck) {
			// are we searching for null?
			addNullCheck = IQueryInterpreter.getAllSearchComparators().contains(thisComparator) && 
					(objects.contains("n") || objects.contains("nu") || objects.contains("nul") || objects.contains("null"));
		}
		
		StringBuilder filterBuilder = null;
		// add the null check now
		if(addNullCheck) {
			if(thisComparator.equals("==") || IQueryInterpreter.getPosSearchComparators().contains(thisComparator)) {
				filterBuilder = new StringBuilder();
				filterBuilder.append("( (").append(leftSelectorExpression).append(") IS NULL ");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>") || IQueryInterpreter.getNegSearchComparators().contains(thisComparator)) {
				nullCheckWithEquals = false;
				filterBuilder = new StringBuilder();
				filterBuilder.append("( (").append(leftSelectorExpression).append(") IS NOT NULL ");
			}
		}
		// if there are other instances as well
		// also add that
		// if objects is empty but we didn't add the null check
		// then we will filter on nothing
		if(!objects.isEmpty() || !addNullCheck) {
			if(filterBuilder == null) {
				filterBuilder = new StringBuilder();
			} else {
				// we added a null check above
				if(nullCheckWithEquals) {
					filterBuilder.append("OR ");
				} else {
					filterBuilder.append("AND ");
				}
			}
			boolean isSearch = IQueryInterpreter.getPosSearchComparators().contains(thisComparator);
			boolean isNotSearch = IQueryInterpreter.getNegSearchComparators().contains(thisComparator);
			if(isSearch || isNotSearch) {
				String thisFilterSearch = " LIKE ";
				if(isNotSearch) {
					thisFilterSearch = " NOT" + thisFilterSearch;
				}
				// like requires OR statements for multiple
				// cannot use same logic as IN :(
				int i = 0;
				int size = objects.size();
				List<Object> newObjects = new ArrayList<>();
				newObjects.add(objects.get(i));
				// always process as string
				String myFilterFormatted = getFormatedObject("STRING", newObjects, thisComparator);
				filterBuilder.append("( LOWER(");
				boolean cast = SemossDataType.convertStringToDataType(leftDataType) != SemossDataType.STRING;
				if(cast) {
					filterBuilder.append("CAST(").append(leftSelectorExpression).append(" as CHAR(50))");
				} else {
					filterBuilder.append(leftSelectorExpression);
				}
				filterBuilder.append(") " + thisFilterSearch + " (").append(myFilterFormatted.toLowerCase()).append(")");
				i++;
				for(; i < size; i++) {
					newObjects = new ArrayList<>();
					newObjects.add(objects.get(i));
					// always process as string
					myFilterFormatted = getFormatedObject("STRING", newObjects, thisComparator);
					filterBuilder.append(" OR LOWER(");
					if(cast) {
						filterBuilder.append("CAST(").append(leftSelectorExpression).append(" as CHAR(50))");
					} else {
						filterBuilder.append(leftSelectorExpression);
					}
					filterBuilder.append(") " + thisFilterSearch + " (").append(myFilterFormatted.toLowerCase()).append(")");
				}
				filterBuilder.append(")");
			} else {
				filterBuilder.append("(").append(leftSelectorExpression).append(")");
				String myFilterFormatted = getFormatedObject(leftDataType, objects, thisComparator);
	
				if(thisComparator.trim().equals("==")) {
					filterBuilder.append(" IN ( ").append(myFilterFormatted).append(" ) ");
				} else if(thisComparator.trim().equals("!=") || thisComparator.equals("<>")) {
					filterBuilder.append(" NOT IN ( ").append(myFilterFormatted).append(" ) ");
				} else {
					filterBuilder.append(" ").append(thisComparator).append(" ").append(myFilterFormatted);
				}
			}
		}
		
		if(addNullCheck && filterBuilder != null) {
			filterBuilder.append(" )");
		}
		
		return filterBuilder;
	}

	/**
	 * Add filter for column to column
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	protected StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		IQuerySelector rightSelector = (IQuerySelector) rightComp.getValue();

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */
		
		StringBuilder filterBuilder = new StringBuilder();
		filterBuilder.append(processSelector(leftSelector, false));
		if(thisComparator.equals("==")) {
			thisComparator = "=";
		} else if(thisComparator.equals("<>")) {
			thisComparator = "!=";
		}
		filterBuilder.append(" ").append(thisComparator).append(" ").append(processSelector(rightSelector, false));

		return filterBuilder;
	}
	
	protected StringBuilder addValueToValueFilter(NounMetadata leftComp, NounMetadata rightComp, String comparator) {
		// WE ARE COMPARING A CONSTANT TO ANOTHER CONSTANT
		// ... what is the point of this... this is a dumb thing... you are dumb

		PixelDataType lCompType = leftComp.getNounType();
		List<Object> leftObjects = new ArrayList<>();
		// ugh... this is gross
		if(leftComp.getValue() instanceof List) {
			leftObjects.addAll( (List) leftComp.getValue());
		} else {
			leftObjects.add(leftComp.getValue());
		}
		String leftDataType = null;
		if(lCompType == PixelDataType.CONST_DECIMAL) {
			leftDataType = "NUMBER";
		} else {
			leftDataType = "STRING";
		}
		String leftFilterFormatted = getFormatedObject(leftDataType, leftObjects, comparator);
		
		
		PixelDataType rCompType = rightComp.getNounType();
		List<Object> rightObjects = new ArrayList<>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof Collection) {
			rightObjects.addAll( (Collection) rightComp.getValue());
		} else {
			rightObjects.add(rightComp.getValue());
		}
		
		String rightDataType = null;
		if(rCompType == PixelDataType.CONST_DECIMAL) {
			rightDataType = "NUMBER";
		} else {
			rightDataType = "STRING";
		}
		String rightFilterFormatted = getFormatedObject(rightDataType, rightObjects, comparator);

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */

		StringBuilder filterBuilder = new StringBuilder();
		filterBuilder.append(leftFilterFormatted);
		if(comparator.equals("==")) {
			comparator = "=";
		} else if(comparator.equals("<>")) {
			comparator = "!=";
		}
		filterBuilder.append(" ").append(comparator).append(" ").append(rightFilterFormatted);

		return filterBuilder;
	}

	/**
	 * This is an optimized version when we know we can get all the objects into 
	 * the proper sql query string in one go
	 * @param dataType
	 * @param objects
	 * @param comparator
	 * @return
	 */
	protected String getFormatedObject(String dataType, List<Object> objects, String comparator) {
		// this will hold the sql acceptable format of the object
		StringBuilder myObj = new StringBuilder();
		
		// defining variables for looping
		int i = 0;
		int size = objects.size();
		if(size == 0) {
			return "";
		}
		// if we can get the data type from the OWL, lets just use that
		// if we dont have it, we will do type casting...
		if(dataType != null) {
			dataType = dataType.toUpperCase();
			SemossDataType type = SemossDataType.convertStringToDataType(dataType);
			if(SemossDataType.INT == type || SemossDataType.DOUBLE == type || SemossDataType.BOOLEAN == type) {
				// get the first value
				myObj.append(objects.get(0));
				i++;
				// loop through all the other values
				for(; i < size; i++) {
					myObj.append(" , ").append(objects.get(i));
				}
			} else if(SemossDataType.DATE == type || SemossDataType.TIMESTAMP == type) {
				String leftWrapper = null;
				String rightWrapper = null;
				boolean isSearch = comparator.equalsIgnoreCase(SEARCH_COMPARATOR) || comparator.equals(NOT_SEARCH_COMPARATOR);
				if(isSearch) {
					leftWrapper = "'%";
					rightWrapper = "%'";
				} else {
					leftWrapper = "'";
					rightWrapper = "'";
				}
				
				// get the first value
				Object val = objects.get(0);
				String d = formatDate(val, type);
				// get the first value
				myObj.append(leftWrapper).append(d).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString();
					d = formatDate(val, type);
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(d).append(rightWrapper);
				}
			} else {
				String leftWrapper = null;
				String rightWrapper = null;
				boolean isSearch = IQueryInterpreter.getAllSearchComparators().contains(comparator);
				if(comparator.equalsIgnoreCase(SEARCH_COMPARATOR) || comparator.equals(NOT_SEARCH_COMPARATOR)) {
					leftWrapper = "'%";
					rightWrapper = "%'";
				} else if(comparator.equalsIgnoreCase(BEGINS_COMPARATOR) || comparator.equals(NOT_BEGINS_COMPARATOR)) {
					leftWrapper = "'";
					rightWrapper = "%'";
				} else if(comparator.equalsIgnoreCase(ENDS_COMPARATOR) || comparator.equals(NOT_ENDS_COMPARATOR)) {
					leftWrapper = "'%";
					rightWrapper = "'";					
				} else {
					leftWrapper = "'";
					rightWrapper = "'";
				}
				
				// get the first value
				String val = AbstractSqlQueryUtil.escapeForSQLStatement(objects.get(i).toString());
				// get the first value
				if(isSearch && val.contains("\\")) {
					myObj.append(leftWrapper).append(val.replace("\\", "\\\\")).append(rightWrapper);
				} else {
					myObj.append(leftWrapper).append(val).append(rightWrapper);
				}
				i++;
				for(; i < size; i++) {
					val = AbstractSqlQueryUtil.escapeForSQLStatement(objects.get(i).toString());
					// get the other values
					if(isSearch && val.contains("\\")) {
						myObj.append(" , ").append(leftWrapper).append(val.replace("\\", "\\\\")).append(rightWrapper);
					} else {
						myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
					}
				}
			}
		} 
		else {
			// do it based on type casting
			// can't have mixed types
			// so only using first value
			Object object = objects.get(0);
			if(object instanceof Number || object instanceof Boolean) {
				// get the first value
				myObj.append(objects.get(0));
				i++;
				// loop through all the other values
				for(; i < size; i++) {
					myObj.append(" , ").append(objects.get(i));
				}
			} else if(object instanceof java.util.Date || object instanceof java.sql.Date) {
				String leftWrapper = null;
				String rightWrapper = null;
				boolean isSearch = comparator.equalsIgnoreCase(SEARCH_COMPARATOR) || comparator.equals(NOT_SEARCH_COMPARATOR);
				if(isSearch) {
					leftWrapper = "'%";
					rightWrapper = "%'";
				} else {
					leftWrapper = "'";
					rightWrapper = "'";
				}
				
				// get the first value
				String val = objects.get(0).toString();
				String d = Utility.getDate(val);
				// get the first value
				myObj.append(leftWrapper).append(d).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString();
					d = Utility.getDate(val);
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(d).append(rightWrapper);
				}
			} else {
				String leftWrapper = null;
				String rightWrapper = null;
				boolean isSearch = IQueryInterpreter.getAllSearchComparators().contains(comparator);
				if(comparator.equalsIgnoreCase(SEARCH_COMPARATOR) || comparator.equals(NOT_SEARCH_COMPARATOR)) {
					leftWrapper = "'%";
					rightWrapper = "%'";
				} else if(comparator.equalsIgnoreCase(BEGINS_COMPARATOR) || comparator.equals(NOT_BEGINS_COMPARATOR)) {
					leftWrapper = "'";
					rightWrapper = "%'";
				} else if(comparator.equalsIgnoreCase(ENDS_COMPARATOR) || comparator.equals(NOT_ENDS_COMPARATOR)) {
					leftWrapper = "'%";
					rightWrapper = "'";					
				} else {
					leftWrapper = "'";
					rightWrapper = "'";
				}
				
				// get the first value
				String val = AbstractSqlQueryUtil.escapeForSQLStatement(objects.get(i).toString());
				// get the first value
				if(isSearch && val.contains("\\")) {
					myObj.append(leftWrapper).append(val.replace("\\", "\\\\")).append(rightWrapper);
				} else {
					myObj.append(leftWrapper).append(val).append(rightWrapper);
				}
				i++;
				for(; i < size; i++) {
					val = AbstractSqlQueryUtil.escapeForSQLStatement(objects.get(i).toString());
					// get the first value
					// get the other values
					if(isSearch && val.contains("\\")) {
						myObj.append(" , ").append(leftWrapper).append(val.replace("\\", "\\\\")).append(rightWrapper);
					} else {
						myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
					}
				}
			}
		}
		
		return myObj.toString();
	}
	
	protected String formatDate(Object o, SemossDataType dateType) {
		if(o instanceof SemossDate) {
			return ((SemossDate) o).getFormattedDate();
		} else {
			if(dateType == SemossDataType.DATE) {
				SemossDate value = SemossDate.genDateObj(o + "");
				if(value != null) {
					return value.getFormatted("yyyy-MM-dd");
				}
			} else {
				SemossDate value = SemossDate.genTimeStampDateObj(o + "");
				if(value != null) {
					return value.getFormatted("yyyy-MM-dd HH:mm:ss");
				}
			}
		}
		return o + "";
	}
	
	////////////////////////////////////// end adding filters ////////////////////////////////////////////

	
	//////////////////////////////////////append order by  ////////////////////////////////////////////
	
	public void addOrderBys() {
		//grab the order by and get the corresponding display name for that order by column
		List<IQuerySort> orderByList = ((SelectQueryStruct) this.qs).getCombinedOrderBy();

		for(IQuerySort orderBy : orderByList) {
			if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.COLUMN) {
				QueryColumnOrderBySelector orderBySelector = (QueryColumnOrderBySelector) orderBy;
				String tableConceptualName = orderBySelector.getTable();
				String columnConceptualName = orderBySelector.getColumn();
				ORDER_BY_DIRECTION orderByDir = orderBySelector.getSortDir();

				boolean origPrim = false;
				if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
					origPrim = true;
					columnConceptualName = getPrimKey4Table(tableConceptualName);
				} else if(this.customFromAliasName==null || this.customFromAliasName.isEmpty()){
					columnConceptualName = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
				}

				StringBuilder thisOrderBy = new StringBuilder();

				// might want to order by a derived column being returned
				if(origPrim && this.selectorAliases.contains(tableConceptualName)) {
					// either instantiate the string builder or add a comma for multi sort
					if(queryUtil.isSelectorKeyword(tableConceptualName)) {
						thisOrderBy.append(queryUtil.getEscapeKeyword(tableConceptualName));
					} else {
						thisOrderBy.append(queryUtil.escapeReferencedAlias(tableConceptualName));
					}
				}
				// account for custom from + sort is a valid column being returned
				else if(this.customFromAliasName != null && !this.customFromAliasName.isEmpty()) {
					String orderByTable = this.customFromAliasName;
					String orderByColumn = queryUtil.escapeReferencedAlias(columnConceptualName);

					if(this.retTableToCols.get(orderByTable).contains(orderByColumn)) {
						thisOrderBy.append(orderByTable).append(".").append(orderByColumn);
					} else {
						continue;
					}
				}
				// account for sort being on table/column being returned
				else if(this.retTableToCols.containsKey(tableConceptualName) && 
						this.retTableToCols.get(tableConceptualName).contains(columnConceptualName)) 
				{
					// these are the physical names
					String orderByTable = getAlias(getPhysicalTableNameFromConceptualName(tableConceptualName));
					String orderByColumn = columnConceptualName;

//							if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
//								orderByColumn = getPrimKey4Table(tableConceptualName);
//							} else {
//								orderByColumn = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
//							}

					if(queryUtil.isSelectorKeyword(orderByTable)) {
						orderByTable = queryUtil.getEscapeKeyword(orderByTable);
					}
					if(queryUtil.isSelectorKeyword(orderByColumn)) {
						orderByColumn = queryUtil.getEscapeKeyword(orderByColumn);
					}
					thisOrderBy.append(orderByTable).append(".").append(orderByColumn);
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
				this.orderBys.add(thisOrderBy);
				
			} else if(orderBy.getQuerySortType() == IQuerySort.QUERY_SORT_TYPE.CUSTOM) {
				QueryCustomOrderBy customSort = (QueryCustomOrderBy) orderBy;
				List<Object> customOrder = customSort.getCustomOrder();
				if(customOrder == null || customOrder.isEmpty()) {
					continue;
				}
				
				QueryColumnSelector orderBySelector = customSort.getColumnToSort();
				String tableConceptualName = orderBySelector.getTable();
				String columnConceptualName = orderBySelector.getColumn();

				boolean origPrim = false;
				if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
					origPrim = true;
					columnConceptualName = getPrimKey4Table(tableConceptualName);
				} else if(this.customFromAliasName==null || this.customFromAliasName.isEmpty()){
					columnConceptualName = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
				}

				String oTableName = null;
				String oColumnName = null;
				// might want to order by a derived column being returned
				if(origPrim && this.selectorAliases.contains(tableConceptualName)) {
					// either instantiate the string builder or add a comma for multi sort
					if(queryUtil.isSelectorKeyword(tableConceptualName)) {
						oTableName = queryUtil.getEscapeKeyword(tableConceptualName);
					} else {
						oTableName = queryUtil.escapeReferencedAlias(tableConceptualName);
					}
				}
				// account for custom from + sort is a valid column being returned
				else if(this.customFromAliasName != null && !this.customFromAliasName.isEmpty()) {
					String orderByTable = this.customFromAliasName;
					String orderByColumn = queryUtil.escapeReferencedAlias(columnConceptualName);

					if(this.retTableToCols.get(orderByTable).contains(orderByColumn)) {
						oTableName = orderByTable;
						oColumnName = orderByColumn;
					} else {
						continue;
					}
				}
				// account for sort being on table/column being returned
				else if(this.retTableToCols.containsKey(tableConceptualName) && 
						this.retTableToCols.get(tableConceptualName).contains(columnConceptualName)) 
				{
					// these are the physical names
					String orderByTable = getAlias(getPhysicalTableNameFromConceptualName(tableConceptualName));
					String orderByColumn = columnConceptualName;

//							if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
//								orderByColumn = getPrimKey4Table(tableConceptualName);
//							} else {
//								orderByColumn = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
//							}

					if(queryUtil.isSelectorKeyword(orderByTable)) {
						orderByTable = queryUtil.getEscapeKeyword(orderByTable);
					}
					if(queryUtil.isSelectorKeyword(orderByColumn)) {
						orderByColumn = queryUtil.getEscapeKeyword(orderByColumn);
					}
					oTableName = orderByTable;
					oColumnName = orderByColumn;
				}
				// well, this is not a valid order by to add
				else {
					continue;
				}
				
				String identifier = oTableName;
				if(oColumnName != null) {
					identifier += "." + oColumnName;
				}
				
				StringBuilder thisSort = createCustomOrderBy(identifier, customOrder);
				this.orderBys.add(thisSort);
				if(((SelectQueryStruct) this.qs).isDistinct()) {
					this.orderBySelectors.add(thisSort);
				}
			}
		}
	}
	
	public StringBuilder appendOrderBy(StringBuilder query) {
		int size = this.orderBys.size();
		for(int i = 0; i < size; i++) {
			if(i == 0) {
				query.append(" ORDER BY ");
			} else {
				query.append(", ");
			}
			query.append(this.orderBys.get(i).toString());
		}
		return query;
	}
	
	/**
	 * Append a custom order by via a CASE WHEN statement
	 * @param identifier
	 * @param values
	 * @return
	 */
	private StringBuilder createCustomOrderBy(String identifier, List<Object> values) {
		int counter = 0;
		StringBuilder builder = new StringBuilder("CASE");
		for(Object val : values) {
			builder.append(" WHEN ").append(identifier);
			if(val == null) {
				builder.append(" IS NULL");
			} else if(val instanceof Number) {
				builder.append("=").append(val);
			} else {
				builder.append("=").append("'").append(AbstractSqlQueryUtil.escapeForSQLStatement(val+"")).append("'");
			}
			
			builder.append(" THEN ").append(counter++);
		}
		builder.append(" END");
		return builder;
	}
	
	//////////////////////////////////////end append order by////////////////////////////////////////////
	
	
	//////////////////////////////////////append group by  ////////////////////////////////////////////
	
	public StringBuilder appendGroupBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
		List<IQuerySelector> groupBy = ((SelectQueryStruct) this.qs).getGroupBy();
		StringBuilder groupByName = new StringBuilder();
		int numGroups = groupBy.size();
		
		QueryColumnSelector queryColumnSelector = null;
		QueryFunctionSelector queryFunctionSelector = null;
		
		for(int i = 0; i < numGroups; i++) {
			IQuerySelector groupBySelector = groupBy.get(i);
			
			String tableConceptualName = null;
			String columnConceptualName = null;
			
			if(groupBySelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.COLUMN) {
				queryColumnSelector = (QueryColumnSelector) groupBySelector;
				tableConceptualName = queryColumnSelector.getTable();
				columnConceptualName = queryColumnSelector.getColumn();
			}
			else if (groupBySelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION) {
				if(i > 0) {
					groupByName.append(", ");
				}
				queryFunctionSelector = (QueryFunctionSelector) groupBySelector;
				groupByName.append(processFunctionSelector(queryFunctionSelector));
				continue;
			} else if (groupBySelector.getSelectorType() == IQuerySelector.SELECTOR_TYPE.IF_ELSE) {
				if(i > 0) {
					groupByName.append(", ");
				}
				groupByName.append(processIfElseSelector((QueryIfSelector) groupBySelector, false, false));
				continue;
			}
			else {
				String errorMessage = "Cannot group by non QueryColumnSelector and QueryFunctionSelector and QueryIfSelector types yet...";
				logger.error(errorMessage);
				throw new IllegalArgumentException(errorMessage);
			}
				
			// these are the physical names
			String groupByTable = null;
			String groupByColumn = null;

			// account for custom from
			if(this.subQsAliasNames.contains(tableConceptualName)) {
				// this is a column selector from a projection off a subquery
				groupByTable = tableConceptualName;
				groupByColumn = columnConceptualName;
			} else if(this.customFromAliasName != null && !this.customFromAliasName.isEmpty()) {
				groupByTable = this.customFromAliasName;
				groupByColumn = queryUtil.escapeReferencedAlias(columnConceptualName);
			} else {
				groupByTable = getAlias(getPhysicalTableNameFromConceptualName(tableConceptualName));
				if(columnConceptualName.equals(SelectQueryStruct.PRIM_KEY_PLACEHOLDER)){
					groupByColumn = getPrimKey4Table(tableConceptualName);
				} else {
					groupByColumn = getPhysicalPropertyNameFromConceptualName(tableConceptualName, columnConceptualName);
				}
			}
			
			// escape reserved words
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
		
		if(numGroups > 0) {
			query.append(" GROUP BY ").append(groupByName);
		}
		return query;
	}
	
	//////////////////////////////////////end append group by////////////////////////////////////////////
	
	//////////////////////////////////// caching utility methods /////////////////////////////////////////
	
	//////////////////////////////////// caching utility methods /////////////////////////////////////////

	/**
	 * Get the physical name of the table
	 * @param tablePixelName						The pixel name of the table
	 * @return
	 */
	protected String getPhysicalTableNameFromConceptualName(String tablePixelName) {
		// if engine present
		// get the appropriate physical storage name for the table
		if(engine != null && !engine.isBasic()) {
			// if we already have it, just grab from hash
			if(conceptualConceptToPhysicalMap.containsKey(tablePixelName)) {
				return conceptualConceptToPhysicalMap.get(tablePixelName);
			}

			// we dont have it.. so query for it
			String physicalTableUri = this.engine.getPhysicalUriFromPixelSelector(tablePixelName);
			// table name is the instance name of the URI
			String tableName = Utility.getInstanceName(physicalTableUri);
			// store the physical name as well in case we get it later
			conceptualConceptToPhysicalMap.put(tablePixelName, tableName);
			return tableName;
		} else {
			// no engine is defined, just return the value
			return tablePixelName;
		}
	}

	/**
	 * Get the physical name for a property
	 * @param tablePixelName						The pixel name of the table
	 * @param columnPixelName						The pixel name of the property
	 * @return										The physical name of the property
	 */
	protected String getPhysicalPropertyNameFromConceptualName(String tablePixelName, String columnPixelName) {
		String pixelName = tablePixelName + "__" + columnPixelName;
		if(engine != null && !engine.isBasic()) {
			// if we already have it, just grab from hash
			if(conceptualPropertyToPhysicalMap.containsKey(pixelName)) {
				return conceptualPropertyToPhysicalMap.get(pixelName);
			}
			// we don't have it... so query for it
			String colURI = this.engine.getPhysicalUriFromPixelSelector(pixelName);
			// the class is the name of the column
			String colName = Utility.getClassName(colURI);
			conceptualPropertyToPhysicalMap.put(pixelName, colName);
			return colName;
		} else {
			// no engine is defined, just return the value
			return columnPixelName;
		}
	}

	/**
	 * Get the primary key from the conceptual table name
	 * @param table						The conceptual table name
	 * @return							The physical table name
	 */
	@Deprecated
	protected String getPrimKey4Table(String conceptualTableName){
		if(primaryKeyCache.containsKey(conceptualTableName)){
			return primaryKeyCache.get(conceptualTableName);
		}
		else if (engine != null && !engine.isBasic()) {
			// we dont have it.. so query for it
			String physicalUri = this.engine.getPhysicalUriFromPixelSelector(conceptualTableName);
			if(physicalUri != null) {
				String primKey = this.engine.getLegacyPrimKey4Table(physicalUri);
				primaryKeyCache.put(conceptualTableName, primKey);
				return primKey;
			}
		}
		return conceptualTableName;
	}

	/**
	 * Get the alias for each table name
	 * @param tableName				The table name
	 * @return						The alias for the table name
	 */
	public String getAlias(String curTableName)
	{
		return curTableName;
		/*
		// try to find if the table name has schema in it
		String [] tableTokens = curTableName.split("[.]");

		// now just take the latest one
		String tableName = tableTokens[tableTokens.length - 1];

		// alias already exists
		if(aliases.containsKey(tableName)) {
			return aliases.get(tableName);
		} else {
			boolean aliasComplete = false;
			int count = 0;
			String tryAlias = "";
			while(!aliasComplete)
			{
				if(tryAlias.length()>0){
					tryAlias+="_"; //prevent an error where you may create an alias that is a reserved word (ie, we did this with "as")
				}
				tryAlias = (tryAlias + tableName.charAt(count)).toUpperCase();
				aliasComplete = !aliases.containsValue(tryAlias);
				count++;
			}
			aliases.put(tableName, tryAlias);
			return tryAlias;
		}
		 */
	}
	
	////////////////////////////// end caching utility methods //////////////////////////////////////
	
	
	/////////////////////////////// other utility methods /////////////////////////////////////////
	
	/**
	 * Gets the 4 parts needed to define a relationship
	 * 1) the start table
	 * 2) the start tables column
	 * 3) the end table
	 * 4) the end tables column
	 * 
	 * We have 3 situations
	 * 1) If all 4 parts are defined within the fromString and toString parameters by utilizing
	 * a "__", then it just converts to the physical names and is done
	 * 2) If startTable and start column is defined but endTable/endColumn is not, it assumes the input
	 * for endString is a concept and should bind on its primary key.  This is analogous for when endTable
	 * and end column are defined but the startString is not.
	 * 3) Neither are defined, so we must use the OWL to define the relationship between the 2 tables
	 * 
	 * @param fromString				The start string defining the table/column
	 * @param toString					The end string defining the table/column
	 * @return							String[] of length 4 where the indices are
	 * 									[startTable, startCol, endTable, endCol]
	 */
	protected List<String[]> getRelationshipConceptProperties(String fromString, String toString){
		if(relationshipConceptPropertiesMap.containsKey(fromString + "__" + toString)) {
			return relationshipConceptPropertiesMap.get(fromString + "__" + toString);
		}
		
		String fromTable = null;
		String fromCol = null;
		String toTable = null;
		String toCol = null;
		
		// see if both the table name and column name are specified for the fromString
		if(fromString.contains("__")){
			String fromConceptualTable = fromString.substring(0, fromString.indexOf("__"));
			String fromConceptualColumn = fromString.substring(fromString.indexOf("__")+2);
			
			// need to make these the physical names
			if(engine != null && !engine.isBasic()) {
				fromTable = getPhysicalTableNameFromConceptualName(fromConceptualTable);
				fromCol = getPhysicalPropertyNameFromConceptualName(fromConceptualTable, fromConceptualColumn);
			} else {
				fromTable = fromConceptualTable;
				fromCol = fromConceptualColumn;
			}
		}
		
		// see if both the table name and column name are specified for the toString
		if(toString.contains("__")){
			String toConceptualTable = toString.substring(0, toString.indexOf("__"));
			String toConceptualColumn = toString.substring(toString.indexOf("__")+2);
			
			// need to make these the physical names
			if(engine != null && !engine.isBasic()) {
				toTable = getPhysicalTableNameFromConceptualName(toConceptualTable);
				toCol = getPhysicalPropertyNameFromConceptualName(toConceptualTable, toConceptualColumn);
			} else {
				toTable = toConceptualTable;
				toCol = toConceptualColumn;
			}
		}
		
		// will return an array of values
		List<String[]> retArr = new ArrayList<>();
		
		// if both have table and property defined, then we know exactly what we need to do
		// for the join... so we are done!
		
		// however, if one has a property specified and the other doesn't
		// then we want to connect the one table with col specified to the other table 
		// using the primary key of that table
		// lets try this for both cases of either toTable or fromTable not being specified 
		if(fromTable != null && toTable == null){
			String[] toConProp = getConceptProperty(toString);
			toTable = toConProp[0];
			toCol = toConProp[1];
			// store the single result
			retArr.add(new String[]{fromTable, fromCol, toTable, toCol});
		}
		
		else if(fromTable == null && toTable != null){
			String[] fromConProp = getConceptProperty(fromString);
			fromTable = fromConProp[0];
			fromCol = fromConProp[1];
			// store the single result
			retArr.add(new String[]{fromTable, fromCol, toTable, toCol});
		}
		
		// if neither has a property specified, use owl to look up foreign key relationship
		else if(engine != null && !engine.isBasic() && (fromCol == null && toCol == null)) // in this case neither has a property specified. time to go to owl to get fk relationship
		{
			String fromURI = null;
			String toURI = null;
			
//			String fromConceptual = "http://semoss.org/ontologies/Concept/" + fromString;
//			String toConceptual = "http://semoss.org/ontologies/Concept/" + toString;

			fromURI = this.engine.getPhysicalUriFromPixelSelector(fromString);
			toURI = this.engine.getPhysicalUriFromPixelSelector(toString);

			// need to figure out what the predicate is from the owl
			// also need to determine the direction of the relationship -- if it is forward or backward
			String query = "SELECT ?relationship WHERE {<" + fromURI + "> ?relationship <" + toURI + "> } ORDER BY DESC(?relationship)";
			TupleQueryResult res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
			String predURI = " unable to get pred from owl for " + fromURI + " and " + toURI;
			try {
				if(!res.hasNext()){
					query = "SELECT ?relationship WHERE {<" + toURI + "> ?relationship <" + fromURI + "> } ORDER BY DESC(?relationship)";
					if(logger.isDebugEnabled()) {
						logger.debug("Relationship query " + query);
					}
					res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
				}
				
				// now loop through all of them
				while(res.hasNext()) {
					predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
					// ignore for silly reflection
					if(predURI.equals("http://semoss.org/ontologies/Relation")) {
						continue;
					}
					
					String[] predPieces = Utility.getInstanceName(predURI).split("[.]");
					if(predPieces.length == 4)
					{
						fromTable = predPieces[0];
						fromCol = predPieces[1];
						toTable = predPieces[2];
						toCol = predPieces[3];
					}
					else if(predPieces.length == 6) // this is coming in with the schema
					{
						// EHUB_CLM_SDS . EHUB_CLM_EVNT . CLM_EVNT_KEY . EHUB_CLM_SDS . EHUB_CLM_PROV_DMGRPHC . CLM_EVNT_KEY
						// [0]               [1]            [2]             [3]             [4]                    [5]
						fromTable = predPieces[0] + "." + predPieces[1];
						fromCol = predPieces[2];
						toTable = predPieces[3] + "." + predPieces[4];
						toCol = predPieces[5];
					}
					
					// store all the results
					retArr.add(new String[]{fromTable, fromCol, toTable, toCol});
				}
			} catch (QueryEvaluationException e) {
				logger.error("ERROR in query for metadata ::: predURI = " + predURI);
			}
		}
		// if everything is provided
		else {
			retArr.add(new String[]{fromTable, fromCol, toTable, toCol});
		}
		relationshipConceptPropertiesMap.put(fromString + "__" + toString, retArr);
		return retArr;
	}
	
	
	/**
	 * Returns the physical concept name and property name for a given input
	 * If the input contains a "__" it returns the physical from both the 
	 * the concept and the property
	 * If the input doesn't contain a "__", get the concept and the primary key 
	 * @param concept_property				The input string
	 * @return								String[] containing the concept physical
	 * 										at index 0 and property physical at index 1
	 */
	protected String[] getConceptProperty(String concept_property) {
		String conceptPhysical = null;
		String propertyPhysical = null;
		
		// if it contains a "__"
		// break the string and get the physical for both parts
		if(concept_property.contains("__")) {
			String concept = concept_property.substring(0, concept_property.indexOf("__"));
			String property = concept_property.substring(concept_property.indexOf("__")+2);
			
			conceptPhysical = getPhysicalTableNameFromConceptualName(concept);
			propertyPhysical = getPhysicalPropertyNameFromConceptualName(concept, property);
		} else {
			// if it doesn't contain a "__", then it is just a concept
			// get the physical and the prim key
			conceptPhysical = getPhysicalTableNameFromConceptualName(concept_property);
			propertyPhysical = getPrimKey4Table(concept_property);
		}
		
		return new String[]{conceptPhysical, propertyPhysical};
	}
	
	public List<String> getFilterStatements() {
		return this.filterStatements;
	}
	
	////////////////////////////////////////// end other utility methods ///////////////////////////////////////////
	
	
	///////////////////////////////////////// test method /////////////////////////////////////////////////
	
	public static void main(String[] args) throws Exception {
		// load in the engine
		TestUtilityMethods.loadDIHelper();

		//TODO: put in correct path for your database
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("Movie_RDBMS");
		coreEngine.open(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
	}

	///////////////////////////////////////// end test methods //////////////////////////////////////////////
	

}