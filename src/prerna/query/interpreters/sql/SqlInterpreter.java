package prerna.query.interpreters.sql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.poi.main.HeadersException;
import prerna.query.interpreters.AbstractQueryInterpreter;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector.ORDER_BY_DIRECTION;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.test.TestUtilityMethods;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.RdbmsTypeEnum;
import prerna.util.sql.SQLQueryUtil;

public class SqlInterpreter extends AbstractQueryInterpreter {
	
	protected static HeadersException headerExec = HeadersException.getInstance();
	
	// this keeps the table aliases
	protected HashMap<String, String> aliases = new HashMap<String, String>();
	
	// keep track of processed tables used to ensure we don't re-add tables into the from string
	protected HashMap<String, String> tablesProcessed = new HashMap<String, String>();
	
	// we will keep track of the conceptual names to physical names so we don't re-query the owl multiple times
	protected transient Map<String, String> conceptualConceptToPhysicalMap = new HashMap<String, String>();
	// need to also keep track of the properties
	protected transient Map<String, String> conceptualPropertyToPhysicalMap = new HashMap<String, String>();
	// need to keep track of the primary key for tables
	protected transient Map<String, String> primaryKeyCache = new HashMap<String, String>();

	// we can create a statement without an engine... 
	// but everything needs to be the physical schema
	protected transient IEngine engine; 
	protected transient ITableDataFrame frame;
	
	// where the wheres are all kept
	// key is always a combination of concept and comparator
	// and the values are values
	protected List<String> filterStatements = new Vector<String>();
	protected List<String> havingFilterStatements = new Vector<String>();
	
	protected transient Map<String, String[]> relationshipConceptPropertiesMap = new HashMap<String, String[]>();
	
	protected String selectors = "";
	protected Set<String> selectorList = new HashSet<String>();
	// keep selector alias
	protected List<String> selectorAliases = new Vector<String>();
	// keep list of columns for tables
	protected Map<String, List<String>> retTableToCols = new HashMap<String, List<String>>();
	
	protected List<String[]> froms = new Vector<String[]>();
	// store the joins in the object for easy use
	protected SqlJoinStructList joinStructList = new SqlJoinStructList();
	
	protected SQLQueryUtil queryUtil = SQLQueryUtil.initialize(RdbmsTypeEnum.H2_DB);
	
	public SqlInterpreter() {
		
	}

	public SqlInterpreter(IEngine engine) {
		this.engine = engine;
		queryUtil = SQLQueryUtil.initialize(((RDBMSNativeEngine) engine).getDbType());
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
				String physicalUri = engine.getConceptPhysicalUriFromConceptualUri(table);
				if( (engine.getConcepts(false).size() == 1) && (engine.getProperties4Concept(physicalUri, false).size() + 1) == selectorList.size()) {
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

		query = appendOrderBy(query);
		
		long limit = ((SelectQueryStruct) this.qs).getLimit();
		long offset = ((SelectQueryStruct) this.qs).getOffset();
		
		query = this.queryUtil.addLimitOffsetToQuery(query, limit, offset);
		
		if(query.length() > 500) {
			logger.debug("SQL QUERY....  " + query.substring(0,  500) + "...");
		} else {
			logger.debug("SQL QUERY....  " + query);
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
		}
		return null;
	}

	protected String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof Number) {
			return constant.toString();
		} else {
			return "'" + RdbmsQueryBuilder.escapeForSQLStatement(constant + "") + "'";
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
		if(tableAlias == null) {
			tableAlias = getAlias(getPhysicalTableNameFromConceptualName(table));
		}
		// will be getting the physical column name
		String physicalColName = colName;
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
		
		// need to perform this check 
		// if there are no joins
		// we need to have a from table
		if(this.joinStructList.isEmpty()) {
			addFrom(table, tableAlias);
		}
		
		// keep track of all the processed columns
		if(notEmbeddedColumn) {
			this.retTableToCols.putIfAbsent(table, new Vector<String>());
			this.retTableToCols.get(table).add(colName);
		}
		
		// if its an illegal char
		// first,  you are a jerk for how you made your table
		// but we will just put quotes around it
		String quote = "";
		if(headerExec.isIllegalHeader(physicalColName)) {
			quote = "\"";
		}
		
		return tableAlias + "." + quote + physicalColName + quote;
	}
	
	protected String processFunctionSelector(QueryFunctionSelector selector) {
		List<IQuerySelector> innerSelectors = selector.getInnerSelector();
		String function = selector.getFunction();
		
		StringBuilder expression = new StringBuilder();
		expression.append(QueryFunctionHelper.convertFunctionToSqlSyntax(function)).append("(");
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
			
			froms.add(new String[]{physicalTableName, alias});
		}
	}

	////////////////////////////////////// end adding from ///////////////////////////////////////
	
	
	////////////////////////////////////// adding joins /////////////////////////////////////////////
	
	/**
	 * Adds the joins for the query
	 */
	public void addJoins() {
		for(String[] relationsData : qs.getRelations()) {
			addJoin(relationsData[0], relationsData[1], relationsData[2]);
		}
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

	protected StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter) {
		NounMetadata leftComp = filter.getLComparison();
		NounMetadata rightComp = filter.getRComparison();
		String thisComparator = filter.getComparator();
		
		FILTER_TYPE fType = filter.getFilterType();
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
		} else if(fType == FILTER_TYPE.VALUE_TO_VALUE) {
			// WHY WOULD YOU DO THIS!!!
			return addValueToValueFilter(rightComp, leftComp, thisComparator);
		}
		return null;
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
			e.printStackTrace();
		}
		if(this.frame != null) {
			subQs = QSAliasToPhysicalConverter.getPhysicalQs(subQs, this.frame.getMetaData());
		}
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
			String left_concept_property = leftSelector.getQueryStructName();
			String[] leftConProp = getConceptProperty(left_concept_property);
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
				leftDataType = this.frame.getMetaData().getHeaderTypeAsString(left_concept_property);
			}
		}
		
		List<Object> objects = new Vector<Object>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof List) {
			objects.addAll( (List) rightComp.getValue());
		} else {
			objects.add(rightComp.getValue());
		}
		
		// need to account for null inputs
		boolean addNullCheck = false;
		if(objects.contains(null)) {
			addNullCheck = true;
			objects.remove(null);
		}
		
		StringBuilder filterBuilder = null;
		// add the null check now
		if(addNullCheck) {
			// can only work if comparator is == or !=
			if(thisComparator.equals("==")) {
				filterBuilder = new StringBuilder();
				filterBuilder.append("( (").append(leftSelectorExpression).append(") IS NULL ");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
				filterBuilder = new StringBuilder();
				filterBuilder.append("( (").append(leftSelectorExpression).append(") IS NOT NULL ");
			}
		}
		// if there are other instances as well
		// also add that
		if(!objects.isEmpty()) {
			if(filterBuilder == null) {
				filterBuilder = new StringBuilder();
			} else {
				// we added a null check above
				filterBuilder.append("OR ");
			}
			if(thisComparator.trim().equals("?like")) {
				// like requires OR statements for multiple
				// cannot use same logic as IN :(
				int i = 0;
				int size = objects.size();
				List<Object> newObjects = new Vector<Object>();
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
				filterBuilder.append(") LIKE (").append(myFilterFormatted.toLowerCase()).append(")");
				i++;
				for(; i < size; i++) {
					newObjects = new Vector<Object>();
					newObjects.add(objects.get(i));
					// always process as string
					myFilterFormatted = getFormatedObject("STRING", newObjects, thisComparator);
					filterBuilder.append(" OR LOWER(");
					if(cast) {
						filterBuilder.append("CAST(").append(leftSelectorExpression).append(" as CHAR(50))");
					} else {
						filterBuilder.append(leftSelectorExpression);
					}
					filterBuilder.append(") LIKE (").append(myFilterFormatted.toLowerCase()).append(")");
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
		
		if(addNullCheck) {
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
		List<Object> leftObjects = new Vector<Object>();
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
		//TODO: NEED TO CONSIDER DATES!!!
		String leftFilterFormatted = getFormatedObject(leftDataType, leftObjects, comparator);
		
		
		PixelDataType rCompType = rightComp.getNounType();
		List<Object> rightObjects = new Vector<Object>();
		// ugh... this is gross
		if(rightComp.getValue() instanceof List) {
			rightObjects.addAll( (List) rightComp.getValue());
		} else {
			rightObjects.add(rightComp.getValue());
		}
		
		String rightDataType = null;
		if(rCompType == PixelDataType.CONST_DECIMAL) {
			rightDataType = "NUMBER";
		} else {
			rightDataType = "STRING";
		}
		//TODO: NEED TO CONSIDER DATES!!!
		String rightFilterFormatted = getFormatedObject(rightDataType, rightObjects, comparator);

		/*
		 * Add the filter syntax here once we have the correct physical names
		 */
		
		StringBuilder filterBuilder = new StringBuilder();
		filterBuilder.append(leftFilterFormatted.toString());
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
			if(SemossDataType.INT == type || SemossDataType.DOUBLE == type) {
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
				if(!comparator.equalsIgnoreCase(SEARCH_COMPARATOR)) {
					leftWrapper = "\'";
					rightWrapper = "\'";
				} else {
					leftWrapper = "'%";
					rightWrapper = "%'";
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
				if(!comparator.equalsIgnoreCase(SEARCH_COMPARATOR)) {
					leftWrapper = "\'";
					rightWrapper = "\'";
				} else {
					leftWrapper = "'%";
					rightWrapper = "%'";
				}
				
				// get the first value
				String val = RdbmsQueryBuilder.escapeForSQLStatement(objects.get(i).toString());
				// get the first value
				myObj.append(leftWrapper).append(val).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = RdbmsQueryBuilder.escapeForSQLStatement(objects.get(i).toString());
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
				}
			}
		} 
		else {
			// do it based on type casting
			// can't have mixed types
			// so only using first value
			Object object = objects.get(0);
			if(object instanceof Number) {
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
				if(!comparator.equalsIgnoreCase(SEARCH_COMPARATOR)) {
					leftWrapper = "\'";
					rightWrapper = "\'";
				} else {
					leftWrapper = "'%";
					rightWrapper = "%'";
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
				if(!comparator.equalsIgnoreCase(SEARCH_COMPARATOR)) {
					leftWrapper = "\'";
					rightWrapper = "\'";
				} else {
					leftWrapper = "'%";
					rightWrapper = "%'";
				}
				
				// get the first value
				String val = RdbmsQueryBuilder.escapeForSQLStatement(objects.get(i).toString());
				// get the first value
				myObj.append(leftWrapper).append(val).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = RdbmsQueryBuilder.escapeForSQLStatement(objects.get(i).toString());
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
				}
			}
		}
		
		return myObj.toString();
	}
	
	private String formatDate(Object o, SemossDataType dateType) {
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
		return null;
	}
	
	////////////////////////////////////// end adding filters ////////////////////////////////////////////

	
	//////////////////////////////////////append order by  ////////////////////////////////////////////
	
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
				thisOrderBy.append(tableConceptualName);
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
	
	//////////////////////////////////////end append order by////////////////////////////////////////////
	
	
	//////////////////////////////////////append group by  ////////////////////////////////////////////
	
	public StringBuilder appendGroupBy(StringBuilder query) {
		//grab the order by and get the corresponding display name for that order by column
		List<QueryColumnSelector> groupBy = ((SelectQueryStruct) this.qs).getGroupBy();
		String groupByName = null;
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
		
		if(groupByName != null) {
			query.append(" GROUP BY ").append(groupByName);
		}
		return query;
	}
	
	//////////////////////////////////////end append group by////////////////////////////////////////////
	
	//////////////////////////////////// caching utility methods /////////////////////////////////////////
	
	/**
	 * Get the physical name of the 
	 * @param conceptualTableName
	 * @return
	 */
	protected String getPhysicalTableNameFromConceptualName(String conceptualTableName) {
		// if engine present
		// get the appropriate physical storage name for the table
		if(engine != null && !engine.isBasic()) {
			// if we already have it, just grab from hash
			if(conceptualConceptToPhysicalMap.containsKey(conceptualTableName)) {
				return conceptualConceptToPhysicalMap.get(conceptualTableName);
			}
			// we dont have it.. so query for it
			String tableURI = this.engine.getConceptPhysicalUriFromConceptualUri(conceptualTableName);
			// table name is the instance name of the URI
			String tableName = Utility.getInstanceName(tableURI);

			// since we also have the URI, just store the primary key as well if we haven't already
			if(!primaryKeyCache.containsKey(conceptualTableName)) {
				// will most likely be used
				String primKey = Utility.getClassName(tableURI);
				primaryKeyCache.put(conceptualTableName, primKey);
			}
			
			// store the physical name as well in case we get it later
			conceptualConceptToPhysicalMap.put(conceptualTableName, tableName);
			return tableName;
		} else {
			// no engine is defined, just return the value
			return conceptualTableName;
		}
	}
	
	/**
	 * Get the physical name for a property
	 * @param columnConceptualName					The conceptual name of the property
	 * @return										The physical name of the property
	 */
	protected String getPhysicalPropertyNameFromConceptualName(String tableConceptualName, String columnConceptualName) {
		if(engine != null && !engine.isBasic()) {
			// if we already have it, just grab from hash
			if(conceptualPropertyToPhysicalMap.containsKey(tableConceptualName+columnConceptualName)) {
				return conceptualPropertyToPhysicalMap.get(tableConceptualName+columnConceptualName);
			}
			
			// we don't have it... so query for it
			String colURI = this.engine.getPropertyPhysicalUriFromConceptualUri(columnConceptualName, tableConceptualName);
			// if the user has defined a column 
			// but its not actually part of the OWL 
			// this is annoying since we do this for the _FK columns
			// in the addJoins in the NoOuterJoin class
			if(colURI == null) {
				return columnConceptualName;
			}
			// the class is the name of the column
			String colName = Utility.getClassName(colURI);
			conceptualPropertyToPhysicalMap.put(tableConceptualName+columnConceptualName, colName);
			return colName;
		} else {
			// no engine is defined, just return the value
			return columnConceptualName;
		}
	}
	
	/**
	 * Get the primary key from the conceptual table name
	 * @param table						The conceptual table name
	 * @return							The physical table name
	 */
	protected String getPrimKey4Table(String conceptualTableName){
		if(primaryKeyCache.containsKey(conceptualTableName)){
			return primaryKeyCache.get(conceptualTableName);
		}
		else if (engine != null && !engine.isBasic()) {
			// we dont have it.. so query for it
			String tableURI = this.engine.getConceptPhysicalUriFromConceptualUri(conceptualTableName);
			// since we also have the URI, just store the primary key as well
			// will most likely be used
			String primKey = Utility.getClassName(tableURI);
			primaryKeyCache.put(conceptualTableName, primKey);
			return primKey;
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
//		// try to find if the table name has schema in it
//		String [] tableTokens = curTableName.split("[.]");
//	
//		// now just take the latest one
//		String tableName = tableTokens[tableTokens.length - 1];
//		
//		// alias already exists
//		if(aliases.containsKey(tableName)) {
//			return aliases.get(tableName);
//		} else {
//			boolean aliasComplete = false;
//			int count = 0;
//			String tryAlias = "";
//			while(!aliasComplete)
//			{
//				if(tryAlias.length()>0){
//					tryAlias+="_"; //prevent an error where you may create an alias that is a reserved word (ie, we did this with "as")
//				}
//				tryAlias = (tryAlias + tableName.charAt(count)).toUpperCase();
//				aliasComplete = !aliases.containsValue(tryAlias);
//				count++;
//			}
//			aliases.put(tableName, tryAlias);
//			return tryAlias;
//		}
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
	protected String[] getRelationshipConceptProperties(String fromString, String toString){
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
		}
		
		else if(fromTable == null && toTable != null){
			String[] fromConProp = getConceptProperty(fromString);
			fromTable = fromConProp[0];
			fromCol = fromConProp[1];
		}
		
		// if neither has a property specified, use owl to look up foreign key relationship
		else if(engine != null && !engine.isBasic() && (fromCol == null && toCol == null)) // in this case neither has a property specified. time to go to owl to get fk relationship
		{
			String fromURI = null;
			String toURI = null;
			
			String fromConceptual = "http://semoss.org/ontologies/Concept/" + fromString;
			String toConceptual = "http://semoss.org/ontologies/Concept/" + toString;
			
			fromURI = this.engine.getConceptPhysicalUriFromConceptualUri(fromConceptual);
			toURI = this.engine.getConceptPhysicalUriFromConceptualUri(toConceptual);

			// need to figure out what the predicate is from the owl
			// also need to determine the direction of the relationship -- if it is forward or backward
			String query = "SELECT ?relationship WHERE {<" + fromURI + "> ?relationship <" + toURI + "> } ORDER BY DESC(?relationship)";
			System.out.println(query);
			TupleQueryResult res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
			String predURI = " unable to get pred from owl for " + fromURI + " and " + toURI;
			try {
				if(res.hasNext()){
					predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
				}
				else {
					query = "SELECT ?relationship WHERE {<" + toURI + "> ?relationship <" + fromURI + "> } ORDER BY DESC(?relationship)";
					System.out.println(query);
					res = (TupleQueryResult) this.engine.execOntoSelectQuery(query);
					if(res.hasNext()){
						predURI = res.next().getBinding(res.getBindingNames().get(0)).getValue().toString();
					}
				}
			} catch (QueryEvaluationException e) {
				System.out.println(predURI);
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
		}
		
		String[] retArr = new String[]{fromTable, fromCol, toTable, toCol};
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
	
	
	////////////////////////////////////////// end other utility methods ///////////////////////////////////////////
	
	
	///////////////////////////////////////// test method /////////////////////////////////////////////////
	
	public static void main(String[] args) {
		// load in the engine
		TestUtilityMethods.loadDIHelper();

		//TODO: put in correct path for your database
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie_RDBMS.smss";
		RDBMSNativeEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("Movie_RDBMS");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie_RDBMS", coreEngine);
	}

	///////////////////////////////////////// end test methods //////////////////////////////////////////////
	

}