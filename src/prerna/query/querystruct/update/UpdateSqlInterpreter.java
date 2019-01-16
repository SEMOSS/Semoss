package prerna.query.querystruct.update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IEngine;
import prerna.query.interpreters.sql.SqlInterpreter;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter.FILTER_TYPE;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryArithmeticSelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.selectors.QueryConstantSelector;
import prerna.query.querystruct.selectors.QueryFunctionHelper;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.query.querystruct.selectors.QueryOpaqueSelector;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class UpdateSqlInterpreter {
	
	// keep track of processed tables used to ensure we don't re-add tables into the from string
	private Hashtable<String, String> tableProcessed = new Hashtable<String, String>();
	// need to keep track of the primary key for tables
	private transient Map<String, String> primaryKeyCache = new HashMap<String, String>();
	
	private transient ITableDataFrame frame;
	private transient IEngine engine;
	
	private UpdateQueryStruct qs;
	
	private List<String> filterStatements = new Vector<String>();

	private StringBuilder selectorBuilder = new StringBuilder();
	private StringBuilder sets = new StringBuilder();
	private Map<String, List<String>> retTableToCols = new HashMap<String, List<String>>();
	
	// From other classes
	private String SEARCH_COMPARATOR = "?like";
	protected Logger logger = null;

	private List<String[]> froms = new Vector<String[]>();
	
	public UpdateSqlInterpreter(UpdateQueryStruct qs) {
		this.qs = qs;
		this.frame = qs.getFrame();
		this.engine = qs.getEngine();
	}
	
	//////////////////////////////////////////// Compose Query //////////////////////////////////////////////

	public String composeQuery() {
		addSelectors();
		addSets();
		addFilters();
		
		// Initiate String
		StringBuilder query = new StringBuilder("UPDATE ");
		
		// Add sets depending on...
		query.append(selectorBuilder);
		query.append(" SET ").append(sets);
		
		int numFilters = this.filterStatements.size();
		for(int i = 0; i < numFilters; i++) {
			if(i == 0) {
				query.append(" WHERE ");
			} else {
				query.append(" AND ");
			}
			query.append(this.filterStatements.get(i).toString());
		}
						
		return query.toString();
	}
	
	//////////////////////////////////////////// End Compose Query //////////////////////////////////////////
	
	//////////////////////////////////////////// Add Selectors //////////////////////////////////////////////
	public void addSelectors() {
		List<IQuerySelector> selectors = qs.getSelectors();
		int numSelectors = selectors.size();
		
		List<String> tableList = new Vector<String>();
		for(int i = 0; i < numSelectors; i++) {
			QueryColumnSelector t = (QueryColumnSelector) selectors.get(i);
			String table = t.getTable();
			// do not include tables more than once
			if(tableList.contains(table)) {
				continue;
			}
			
			if(i != 0) {
				selectorBuilder.append(", ");
			}
			selectorBuilder.append(table);
			
			// add to the list so we dont readd
			tableList.add(table);
		}
	}
	
	private void addSets() {
		List<IQuerySelector> selectors = qs.getSelectors();
		List<Object> values = qs.getValues();
		int numSelectors = selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			if(i != 0) {
				sets.append(", ");
			}
			QueryColumnSelector s = (QueryColumnSelector) selectors.get(i);
			String table = s.getTable();
			String column = s.getColumn();
			if(column.equals(AbstractQueryStruct.PRIM_KEY_PLACEHOLDER)) {
				column = getPrimKey4Table(table);
			}
			Object v = values.get(i);
			if(v instanceof String) {
				sets.append(table + "." + column + "=" + "'" + RdbmsQueryBuilder.escapeForSQLStatement(v + "") + "'");
			} else if(v instanceof SemossDate) {
				sets.append(table + "." + column + "=" + "'" + ((SemossDate) v).getFormattedDate() + "'");
			} else {
				sets.append(table + "." + column + "=" + v );
			}
		}
	}
	
	/**
	 * Method is used to generate the appropriate syntax for each type of selector
	 * Note, this returns everything without the alias since this is called again from
	 * the base methods it calls to allow for complex math expressions
	 * @param selector
	 * @return
	 */
	private String processSelector(IQuerySelector selector, boolean addProcessedColumn) {
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
	private String processConstantSelector(QueryConstantSelector selector) {
		Object constant = selector.getConstant();
		if(constant instanceof Number) {
			return constant.toString();
		} else {
			return "\"" + constant + "\"";
		}
	}

	/**
	 * The second
	 * @param selector
	 * @param isTrueColumn
	 * @return
	 */
	private String processColumnSelector(QueryColumnSelector selector, boolean notEmbeddedColumn) {
		String table = selector.getTable();
		String colName = selector.getColumn();
		String tableAlias = selector.getTableAlias();
		if(tableAlias == null) {
			tableAlias = getAlias(getPhysicalTableNameFromConceptualName(table));
		}
		// will be getting the physical column name
		String physicalColName = colName;
		
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
		addFrom(table, tableAlias);
		
		// keep track of all the processed columns
		if(notEmbeddedColumn) {
			this.retTableToCols.putIfAbsent(table, new Vector<String>());
			this.retTableToCols.get(table).add(colName);
		}
		
		return tableAlias + "." + physicalColName;
	}
	
	private String processFunctionSelector(QueryFunctionSelector selector) {
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
		expression.append(")");
		return expression.toString();
	}
	
	private String processArithmeticSelector(QueryArithmeticSelector selector) {
		IQuerySelector leftSelector = selector.getLeftSelector();
		IQuerySelector rightSelector = selector.getRightSelector();
		String mathExpr = selector.getMathExpr();
		
		if(mathExpr.equals("/")) {
			return "(" + processSelector(leftSelector, false) + " " + mathExpr + " NULLIF(" + processSelector(rightSelector, false) + ",0))";
		} else {
			return "(" + processSelector(leftSelector, false) + " " + mathExpr + " " + processSelector(rightSelector, false) + ")";
		}
	}
	
	private String processOpaqueSelector(QueryOpaqueSelector selector) {
		addFrom(selector.getTable(), selector.getTable());
		return selector.getQuerySelectorSyntax();
	}
	//////////////////////////////////////////// End Add Selectors //////////////////////////////////////////

	
	//////////////////////////////////////////// Add From ///////////////////////////////////////////////////
	/**
	 * Adds the form statement for each table
	 * @param conceptualTableName			The name of the table
	 */
	private void addFrom(String conceptualTableName, String alias) {
		// need to determine if we can have multiple froms or not
		// we don't want to add the from table multiple times as this is invalid in sql
		if(!tableProcessed.containsKey(conceptualTableName)) {
			tableProcessed.put(conceptualTableName, "true");
			
			// we want to use the physical table name
			String physicalTableName = getPhysicalTableNameFromConceptualName(conceptualTableName);
			
			froms.add(new String[]{physicalTableName, alias});
		}
	}
	public void addFilters() {
		GenRowFilters cFilters = qs.getCombinedFilters();
		List<IQueryFilter> filters = cFilters.getFilters();
		for(IQueryFilter filter : filters) {
			StringBuilder filterSyntax = processFilter(filter);
			if(filterSyntax != null) {
				this.filterStatements.add(filterSyntax.toString());
			}
		}
	}
	
	private StringBuilder processFilter(IQueryFilter filter) {
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
	
	private StringBuilder processOrQueryFilter(OrQueryFilter filter) {
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

	private StringBuilder processAndQueryFilter(AndQueryFilter filter) {
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

	private StringBuilder processSimpleQueryFilter(SimpleQueryFilter filter) {
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
	
	private StringBuilder addSelectorToQueryFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftSelectorExpression = processSelector(leftSelector, false);
		
		SelectQueryStruct subQs = (SelectQueryStruct) rightComp.getValue();
		SqlInterpreter innerInterpreter = new SqlInterpreter();
		innerInterpreter.setQueryStruct(subQs);
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
	private StringBuilder addSelectorToValuesFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
		thisComparator = thisComparator.trim();
		// get the left side
		IQuerySelector leftSelector = (IQuerySelector) leftComp.getValue();
		String leftSelectorExpression = processSelector(leftSelector, false);
		String leftDataType = leftSelector.getDataType();
		
		// if it is null, then we know we have a column
		// need to grab from metadata
//		if(leftDataType == null) {
//			String left_concept_property = leftSelector.getQueryStructName();
//			String[] leftConProp = getConceptProperty(left_concept_property);
//		}
		
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
				filterBuilder.append("(").append(leftSelectorExpression).append(") IS NULL ");
			} else if(thisComparator.equals("!=") || thisComparator.equals("<>")) {
				filterBuilder = new StringBuilder();
				filterBuilder.append("(").append(leftSelectorExpression).append(") IS NOT NULL ");
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
				String myFilterFormatted = getFormatedObject(leftDataType, newObjects, thisComparator);
				filterBuilder.append("( LOWER(").append(leftSelectorExpression);
				filterBuilder.append(") LIKE (").append(myFilterFormatted.toLowerCase()).append(")");
				i++;
				for(; i < size; i++) {
					newObjects = new Vector<Object>();
					newObjects.add(objects.get(i));
					myFilterFormatted = getFormatedObject(leftDataType, newObjects, thisComparator);
					filterBuilder.append(" OR LOWER(").append(leftSelectorExpression);
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
		
		return filterBuilder;
	}
	
	/**
	 * Add filter for column to column
	 * @param leftComp
	 * @param rightComp
	 * @param thisComparator
	 */
	private StringBuilder addSelectorToSelectorFilter(NounMetadata leftComp, NounMetadata rightComp, String thisComparator) {
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
	
	private StringBuilder addValueToValueFilter(NounMetadata leftComp, NounMetadata rightComp, String comparator) {
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
	private String getFormatedObject(String dataType, List<Object> objects, String comparator) {
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
			} else if(SemossDataType.DATE == type) {
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
			}else {
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
				String val = objects.get(0).toString().replace("\"", "").replaceAll("'", "''").trim();
				// get the first value
				myObj.append(leftWrapper).append(val).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString().replace("\"", "").replaceAll("'", "''").trim();
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
				String val = objects.get(0).toString().replace("\"", "").replaceAll("'", "''").trim();
				// get the first value
				myObj.append(leftWrapper).append(val).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString().replace("\"", "").replaceAll("'", "''").trim();
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
				}
			}
		}
		
		return myObj.toString();
	}
	
	//////////////////////////////////////////// End Add Filters ////////////////////////////////////////////
	
	//////////////////////////////////////////// Caching Utility Methods ////////////////////////////////////
	/**
	 * Get the physical name of the 
	 * @param conceptualTableName
	 * @return
	 */
	private String getPhysicalTableNameFromConceptualName(String conceptualTableName) {
		return conceptualTableName;
	}
	
	/**
	 * Get the physical name for a property
	 * @param columnConceptualName					The conceptual name of the property
	 * @return										The physical name of the property
	 */
	private String getPhysicalPropertyNameFromConceptualName(String tableConceptualName, String columnConceptualName) {
		return columnConceptualName;
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
		else if(engine != null && !engine.isBasic()) {
			// we dont have it.. so query for it
			String conceptualURI = "http://semoss.org/ontologies/Concept/" + conceptualTableName;
			String tableURI = this.engine.getPhysicalUriFromConceptualUri(conceptualURI);
			
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
	public String getAlias(String curTableName) {
		return curTableName;
	}
	
	//////////////////////////////////////////// End Caching Utility Methods ////////////////////////////////
	
	//////////////////////////////////////////// Other Utility Methods //////////////////////////////////////
	
	/**
	 * Returns the physical concept name and property name for a given input
	 * If the input contains a "__" it returns the physical from both the 
	 * the concept and the property
	 * If the input doesn't contain a "__", get the concept and the primary key 
	 * @param concept_property				The input string
	 * @return								String[] containing the concept physical
	 * 										at index 0 and property physical at index 1
	 */
	private String[] getConceptProperty(String concept_property) {
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

	//////////////////////////////////////////// End Other Utility Methods //////////////////////////////////
	public void setLogger(Logger logger) {
		if(logger != null) {
			this.logger = logger;
		}
	}

	//////////////////////////////////////////// Main function to test //////////////////////////////////////
	
	public static void main(String[] args) {
		// load engine
//		TestUtilityMethods.loadDIHelper("C:/Users/laurlai/workspace/Semoss/RDF_Map.prop");
//		
//		String engineProp = "C:/Users/laurlai/workspace/Semoss/db/LocalMasterDatabase.smss";
//		IEngine coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineName(Constants.LOCAL_MASTER_DB_NAME);
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);
//
//		engineProp = "C:/Users/laurlai/workspace/Semoss/db/MovieDB.smss";
//		coreEngine = new RDBMSNativeEngine();
//		coreEngine.setEngineName("MovieDB");
//		coreEngine.openDB(engineProp);
//		DIHelper.getInstance().setLocalProperty("MovieDB", coreEngine);
		
		
		// Create qs object
		UpdateQueryStruct qs = new UpdateQueryStruct();
		
		/**
		 * Update one column on one table
		 */
		qs.addSelector("Nominated", "Nominated");
		List<Object> values = new ArrayList<Object>();
		values.add("N");
		qs.setValues(values);
		QueryColumnSelector tab = new QueryColumnSelector("Nominated__Title_FK");
		NounMetadata fil1 = new NounMetadata(tab, PixelDataType.COLUMN);
		NounMetadata fil2 = new NounMetadata("Chocolat", PixelDataType.CONST_STRING);
		SimpleQueryFilter filter1 = new SimpleQueryFilter(fil2, "=", fil1);
		qs.addExplicitFilter(filter1);
		
		/**
		 * Update one table using values of another for reference
		 * UPDATE Genre SET Genre.Genre='Comedy' WHERE Genre.Title_FK IN (SELECT Title.Title FROM Title WHERE Title = 'Avatar')
		 */
//		qs.addSelector("Genre", "Genre");
//		List<Object> values = new ArrayList<Object>();
//		values.add("Drama");
//		qs.setValues(values);
//		
//		// Making subquery
//		QueryStruct2 subQuery = new QueryStruct2();
//		QueryColumnSelector title = new QueryColumnSelector("Title__Title");
//		subQuery.addSelector(title);
//		NounMetadata fil3 = new NounMetadata(title, PixelDataType.COLUMN);
//		NounMetadata fil4 = new NounMetadata("Avatar", PixelDataType.CONST_STRING);
//		SimpleQueryFilter subQueryFilter = new SimpleQueryFilter(fil3, "=", fil4);
//		subQuery.addExplicitFilter(subQueryFilter);
//		
////		// Add to qs
//		NounMetadata col = new NounMetadata(new QueryColumnSelector("Genre__Title_FK"), PixelDataType.COLUMN);
//		NounMetadata filquery = new NounMetadata(subQuery, PixelDataType.QUERY_STRUCT);
//		SimpleQueryFilter filter5 = new SimpleQueryFilter(col, "==", filquery);
//		qs.addExplicitFilter(filter5);
//				
		// Create interpreter and compose query
		UpdateSqlInterpreter interpreter = new UpdateSqlInterpreter(qs);
		String s = interpreter.composeQuery();
		System.out.println(s);
		
		// run query on engine
//		coreEngine.insertData(s);
//		
//		// viewing results
////		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(coreEngine, "select * from Nominated");
//		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(coreEngine, "select * from Genre");
//		while(it.hasNext()) {
//			System.out.println(Arrays.toString(it.next().getValues()));
//		}
	}
	
	
}
