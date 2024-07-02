package prerna.query.parsers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import prerna.query.parsers.ParamStructDetails.LEVEL;
import prerna.query.parsers.ParamStructDetails.QUOTE;
import prerna.query.querystruct.FunctionExpression;
import prerna.query.querystruct.GenExpression;
import prerna.query.querystruct.OperationExpression;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.util.Constants;

public class GenExpressionWrapper {
	
	private static final Logger classLogger = LogManager.getLogger(GenExpressionWrapper.class);

	// keep table alias
	// this is {alias => table name}
	public Map<String, String> tableAlias = null;
	// keep column alias
	public  Map<String, String> columnAlias = null;
	// used to keep track of every table and column set used
	public  Map<String, Set<String>> schema = null;

	// keeps track of column to select
	public  Map <String, List<GenExpression>> columnSelect = new Hashtable<>();
	// keep track of table to select
	public  Map <String, List<GenExpression>> tableSelect = new Hashtable<>();	
	// keep a list of selects as well
	// to the columns
	public  Map <GenExpression, List<String>> selectColumns = new Hashtable<>();
	
	// groupby hash
	public Map <String, GenExpression> groupByHash = new HashMap<>();
	public Map <String, GenExpression> joinHash = new HashMap<>();

	
	public GenExpression root = null;
	
	// this is the highest level - acctid - when I replace this it will replace this across all of the databases and operators
	// key is column, value is column tables i.e. key to the next level
	public Map<String, List <String>> columnTableIndex = new HashMap<>(); 
	// this is the next highest level - clms.acctid, mbrshp.acctid - this will replace it only for the specified operators not all of it
	// key is column table and value is column table operator i.e. key to the next level
	public Map<String, List <String>> columnTableOperatorIndex = new HashMap<>(); 
	// this is the next level - clms.acctid=, clms.acctid < - this now also includes the unique count
	// key is column table operator and valus is the actual parameter i.e. gen expression
	public Map <String, ParamStructDetails> operatorTableColumnParamIndex = new HashMap<>(); 
	
	 // final level
	public Map <ParamStructDetails, List <GenExpression>> paramToExpressionMap = new HashMap<>();

	public Map <String, ParamStructDetails> paramStringToParamMap = new HashMap<>();

	
	// keeping track of the current operator
	public Stack <String> currentOperator = new Stack<String>();
	public Stack <String> contextExpression = new Stack<String>();
	public Map <String, Boolean> procOrder = new HashMap<String, Boolean>();
	int andCount = 0;
	int orCount = 0;
	int uniqueCounter = 0;
	
	// how many subselects are there
	public int numSubSelects = -1;
	
	// keeps function to expression list
	public Map<String, List<GenExpression>> functionExpressionMapper = new HashMap<String, List<GenExpression>>();
	
	public GenExpressionWrapper()
	{
		tableAlias = new Hashtable <String, String>();
		columnAlias = new Hashtable <String, String>();
		schema = new Hashtable<String, Set<String>>();

	}
	
	public String printOutput() throws Exception
	{
		String finalQuery = root.printQS(root, new StringBuffer()).toString(); 
		return finalQuery;
	}
	
	public void addGroupBy(String columnName, GenExpression expr)
	{
		groupByHash.put(columnName, expr);
	}
	
	public void addJoin(String columnName, GenExpression expr)
	{
		joinHash.put(columnName, expr);
	}
	
	//////////////////////////////// METHOD FOR MANIPULATING PARAMETERS ////////////////////////////////////////////
	
	
	// the key is the table name and the value is the GenExpression that should be used
	public void addRowFilter(Map <String, GenExpression> filterValues)
	{
		// I still need to account for if the user already comes with this
		
		Iterator <String> cols = filterValues.keySet().iterator();
		while(cols.hasNext())
		{
			// get the col
			String thisCol = cols.next();
			GenExpression userFilter = filterValues.get(thisCol);
			
			// see if a select with that table exists
			if(tableSelect.containsKey(thisCol))
			{
				// get the expression and see what is the where clause
				List <GenExpression> selects = tableSelect.get(thisCol);
				for(int selectIndex = 0;selectIndex < selects.size();selectIndex++)
				{
					SelectQueryStruct select = selects.get(selectIndex);
					GenExpression filter = select.filter;
					
					System.out.println("Filter is set to  " + filter);
					if(filter != null)
					{
						GenExpression thisFilter = new GenExpression();
						thisFilter.setOperation(" AND ");
						((GenExpression)filter).paranthesis = true;
						thisFilter.setLeftExpresion(filter);
						// forcing a random opaque one
						userFilter.paranthesis = true;
						thisFilter.setRightExpresion(userFilter);
						// replace with the new filter
						select.filter = thisFilter;
					}
					// add a new filter otherwise
					else
					{
						select.filter = userFilter;						
					}
					
				}
			}
		}
	}
	
	// get the query for a given column in the select
	public String getFilterQuery(String columnName)
	{
		String retQuery = null;
		
		
		return retQuery;
		
	}
	
	// the key is the table name and the value is the GenExpression that should be used
	// I have a query with a few parameters
	// if the parameters are not there then drop it from groupby ?
	// if there are then add it
	// I have a query with multiple groupby, if the groupby is not 
	// the parameter i.e. the column can be - drop
	// 2 blocks
	// columns to be removed
	// columns to be parameterized
	
	public void appendParameter(List <String> columnsToRemove, Map <String, GenExpression> paramValues)
	{
		// I still need to account for if the user already comes with this
		// get the selects for the param columns
		// add them 
		
		// this whole caching strategy is not working
		// I have to just go off every select and do it
		
		// first is the remove
		for(int colIndex = 0;colIndex < columnsToRemove.size();colIndex++)
		{
			// find the alias
			// find the column
			// remove from the select and also from the group by
			
			// this is probably coming through with the alias name
			// get all of the selects and run through it
			Iterator <GenExpression> allSelects = selectColumns.keySet().iterator();
			
			while(allSelects.hasNext())
			{
				
					GenExpression thisSelect = allSelects.next();	
					// remove from the selector
					thisSelect.removeSelect(columnsToRemove.get(colIndex));
					thisSelect.removeGroup(columnsToRemove.get(colIndex));
					
					StringBuffer buff = thisSelect.printQS((GenExpression)thisSelect, new StringBuffer());
				
				// go through every select and replace it 
				// there is a possibility where there are 2 different aliases refering to the same column name ?
				// and this can lead to issues
			}
		}

		// second is add
		// need a way to get to the appropriate selector
		Iterator <String> cols = paramValues.keySet().iterator();
		while(cols.hasNext())
		{
			// get the col
			String thisCol = cols.next();
			GenExpression userFilter = paramValues.get(thisCol);
			
			// see if a select with that table exists
			if(columnSelect.containsKey(thisCol))
			{
				// get the expression and see what is the where clause
				List <GenExpression> selects = columnSelect.get(thisCol);
				for(int selectIndex = 0;selectIndex < selects.size();selectIndex++)
				{
					SelectQueryStruct select = selects.get(selectIndex);
					select.parameterizeColumn(thisCol, userFilter);
				}
			}
		}
	}
	
	public void replaceColumn(String columnName, Object value)
	{
		List <String> tableColumn = columnTableIndex.get(columnName);
		if(tableColumn != null)
		{
			for(int index = 0;index < tableColumn.size();index++)
			{
				replaceTableColumn(tableColumn.get(index), value);
			}
		}
	}
	
	public void replaceTableColumn(String id, Object value)
	{
		List <String> tableColumnOperator = columnTableOperatorIndex.get(id);
		if(tableColumnOperator != null)
		{
			for(int index = 0;index < tableColumnOperator.size();index++)
			{
				replaceTableColumnOperator(tableColumnOperator.get(index), value);
			}
		}
	}
	
	public void replaceTableColumnOperator(String id, Object value)
	{
		if(operatorTableColumnParamIndex.containsKey(id))
		{
			ParamStructDetails tableColumnOperatorParam = operatorTableColumnParamIndex.get(id);
			tableColumnOperatorParam.setCurrentValue(value);
			//replaceParameter(tableColumnOperatorParam);
		}
	}
	
	// fills it with the latest list of parameters
	public void fillParameters()
	{
		Iterator <ParamStructDetails> paramIterator = paramToExpressionMap.keySet().iterator();
		while(paramIterator.hasNext())
		{
			ParamStructDetails daStruct = paramIterator.next();
			// go through the pattern and fill it
			List <GenExpression> exprs = paramToExpressionMap.get(daStruct);
			
			if(exprs != null)
			{
				for(int exprIndex = 0;exprIndex < exprs.size();exprIndex++)
				{
					// get the current value and set it
					StringBuilder finalValue = new StringBuilder();
					String quote = "";
					if(daStruct.getType() == PixelDataType.CONST_STRING)
						quote = "";
					GenExpression thisExpression = exprs.get(exprIndex);
					finalValue.append(quote).append(daStruct.getCurrentValue()).append(quote);
					if(!thisExpression.operation.equalsIgnoreCase("opaque"))
						thisExpression.setLeftExpresion(finalValue.toString());	
					else
						thisExpression.setLeftExpr(finalValue.toString());	
						
				}
			}
		}
	}
	
	/**
	 * Fill the parameters with the user defined names
	 * @param incomingStructs
	 * @param detailsLookup
	 */
	public void fillParameters(List<ParamStructDetails> incomingStructs, Map<ParamStructDetails, ParamStruct> detailsLookup) {
		// first replace the incoming structs with the user defined param names
		for(int paramIndex = 0; paramIndex < incomingStructs.size(); paramIndex++) {
			ParamStructDetails thisStruct = incomingStructs.get(paramIndex);
			LEVEL thisStructLevel = thisStruct.getLevel();
			if(thisStructLevel == LEVEL.DATASOURCE) {
				// not relevant for the query
				continue;
			}
			
			ParamStruct pStruct = detailsLookup.get(thisStruct);
			// normally this is for using [<paramanme>] vs just <paramname>
			// but here we are using it to indicate that quoting is defined by the FE
			boolean noQuote = ParamStruct.PARAM_FILL_USE_ARRAY_TYPES.contains(pStruct.getModelDisplay());
			String userDefinedParamName = pStruct.getParamName();
			
			if(thisStructLevel == LEVEL.COLUMN) {
				// loop through and find all at column level
				for(String key : operatorTableColumnParamIndex.keySet()) {
					ParamStructDetails targetStruct = operatorTableColumnParamIndex.get(key);
					if(targetStruct.getColumnName().equals(thisStruct.getColumnName())) {
						// replace the target struct with the user defined param name
						List <GenExpression> exprs = paramToExpressionMap.get(targetStruct);
						for(int exprIndex = 0; exprIndex < exprs.size(); exprIndex++) {
							// we will replace the existing parameter 
							// again with the parameter name
							// but this time that defined by the user
							String quote = null;
							if(noQuote || targetStruct.getQuote() == QUOTE.NO) {
								quote = "";
							} else if(targetStruct.getQuote() == QUOTE.DOUBLE) {
								quote = "\"";
							} else if(targetStruct.getQuote() == QUOTE.SINGLE) {
								quote = "'";
							}
							String finalValue = quote + "<" + userDefinedParamName + ">" + quote;
							
							GenExpression thisExpression = exprs.get(exprIndex);
							if(!thisExpression.operation.equalsIgnoreCase("opaque")) {
								thisExpression.setLeftExpresion(finalValue);	
							} else {
								thisExpression.setLeftExpr(finalValue);	
							}
						}
						
						// remove this struct from the overall so it wont fill
						paramToExpressionMap.remove(targetStruct);
					}
				}
			} else if(thisStructLevel == LEVEL.TABLE) {
				// loop through and find all at table level
				for(String key : operatorTableColumnParamIndex.keySet()) {
					ParamStructDetails targetStruct = operatorTableColumnParamIndex.get(key);
					if(targetStruct.getColumnName().equals(thisStruct.getColumnName())
							&& targetStruct.getTableName().equals(thisStruct.getTableName())
							) {
						// replace the target struct with the user defined param name
						List <GenExpression> exprs = paramToExpressionMap.get(targetStruct);
						for(int exprIndex = 0; exprIndex < exprs.size(); exprIndex++) {
							// we will replace the existing parameter 
							// again with the parameter name
							// but this time that defined by the user
							String quote = null;
							if(noQuote || targetStruct.getQuote() == QUOTE.NO) {
								quote = "";
							} else if(targetStruct.getQuote() == QUOTE.DOUBLE) {
								quote = "\"";
							} else if(targetStruct.getQuote() == QUOTE.SINGLE) {
								quote = "'";
							}
							String finalValue = quote + "<" + userDefinedParamName + ">" + quote;
							
							GenExpression thisExpression = exprs.get(exprIndex);
							if(!thisExpression.operation.equalsIgnoreCase("opaque")) {
								thisExpression.setLeftExpresion(finalValue);	
							} else {
								thisExpression.setLeftExpr(finalValue);	
							}
						}
						
						// remove this struct from the overall so it wont fill
						paramToExpressionMap.remove(targetStruct);
					}
				}
			} else if(thisStructLevel == LEVEL.OPERATOR) {
				// loop through and find all at operator level
				for(String key : operatorTableColumnParamIndex.keySet()) {
					ParamStructDetails targetStruct = operatorTableColumnParamIndex.get(key);
					if(targetStruct.getColumnName().equals(thisStruct.getColumnName())
							&& targetStruct.getTableName().equals(thisStruct.getTableName())
							&& targetStruct.getOperator().equals(thisStruct.getOperator())
							) {
						// replace the target struct with the user defined param name
						List <GenExpression> exprs = paramToExpressionMap.get(targetStruct);
						for(int exprIndex = 0; exprIndex < exprs.size(); exprIndex++) {
							// we will replace the existing parameter 
							// again with the parameter name
							// but this time that defined by the user
							String quote = null;
							if(noQuote || targetStruct.getQuote() == QUOTE.NO) {
								quote = "";
							} else if(targetStruct.getQuote() == QUOTE.DOUBLE) {
								quote = "\"";
							} else if(targetStruct.getQuote() == QUOTE.SINGLE) {
								quote = "'";
							}
							String finalValue = quote + "<" + userDefinedParamName + ">" + quote;
							
							GenExpression thisExpression = exprs.get(exprIndex);
							if(!thisExpression.operation.equalsIgnoreCase("opaque")) {
								thisExpression.setLeftExpresion(finalValue);	
							} else {
								thisExpression.setLeftExpr(finalValue);	
							}
						}
						
						// remove this struct from the overall so it wont fill
						paramToExpressionMap.remove(targetStruct);
					}
				}
			} else if(thisStructLevel == LEVEL.OPERATORU) {
				String paramStructDetailsKey = thisStruct.getParamKey();
				// compare at u operator level using the param struct details key
				if(operatorTableColumnParamIndex.containsKey(paramStructDetailsKey)){
					ParamStructDetails targetStruct = operatorTableColumnParamIndex.get(paramStructDetailsKey);
					// replace the target struct with the user defined param name
					List <GenExpression> exprs = paramToExpressionMap.get(targetStruct);
					for(int exprIndex = 0; exprIndex < exprs.size(); exprIndex++) {
						// we will replace the existing parameter 
						// again with the parameter name
						// but this time that defined by the user
						String quote = null;
						if(noQuote || targetStruct.getQuote() == QUOTE.NO) {
							quote = "";
						} else if(targetStruct.getQuote() == QUOTE.DOUBLE) {
							quote = "\"";
						} else if(targetStruct.getQuote() == QUOTE.SINGLE) {
							quote = "'";
						}
						String finalValue = quote + "<" + userDefinedParamName + ">" + quote;
						
						GenExpression thisExpression = exprs.get(exprIndex);
						if(!thisExpression.operation.equalsIgnoreCase("opaque")) {
							thisExpression.setLeftExpresion(finalValue);	
						} else {
							thisExpression.setLeftExpr(finalValue);	
						}
					}
					
					// remove this struct from the overall so it wont fill
					paramToExpressionMap.remove(targetStruct);
				}
			}
		}
		
		/*
		 * replace all the other structs with the default values already present in the query
		 * this does not take in inputs
		 * it goes through all the remaining structs generated THROUGH the parsing
		 * and places those back to the default values
		 */
		Iterator <ParamStructDetails> paramIterator = paramToExpressionMap.keySet().iterator();
		while(paramIterator.hasNext()) {
			ParamStructDetails structDetails = paramIterator.next();
			// go through the pattern and fill it
			List <GenExpression> exprs = paramToExpressionMap.get(structDetails);

			if(exprs != null) {
				for(int exprIndex = 0; exprIndex < exprs.size(); exprIndex++) {
					// get the current value and set it
					StringBuilder finalValue = new StringBuilder();
					String quote = "";
					if(structDetails.getType() == PixelDataType.CONST_STRING) {
						quote = "";
					}
					GenExpression thisExpression = exprs.get(exprIndex);
					finalValue.append(quote).append(structDetails.getCurrentValue()).append(quote);
					if(!thisExpression.operation.equalsIgnoreCase("opaque")) {
						thisExpression.setLeftExpresion(finalValue.toString());	
					} else {
						thisExpression.setLeftExpr(finalValue.toString());	
					}
				}
			}
		}
	}
	
	public String makeParameters(String columnName, Object constantValue, String operationName, String actualOperationName, String constantType, GenExpression exprToTrack, String tableName, String defQuery)
	{
		//String tableAliasName = columnName.substring(0, columnName.indexOf("."));
		String tableAliasName = tableName;
		if(tableAlias.containsKey(tableAliasName)) {
			tableName = tableAlias.get(tableAliasName);
		}
		
		//columnName = columnName.replace(tableAliasName + ".", "");
		//columnName = columnName.replace(".", "");

		// add it to the column index first
		List <String> tableColumnList = new Vector<String>();
		if(this.columnTableIndex.containsKey(columnName)) {
			tableColumnList = columnTableIndex.get(columnName);
		}
		
		String tableColumnComposite = tableName + "_" + columnName;

		if(!tableColumnList.contains(tableColumnComposite)) {
			tableColumnList.add(tableColumnComposite);
		}
		columnTableIndex.put(columnName, tableColumnList);

		// next add the operator
		// need to see if the operator exists
		// if so i need to pop and do the left and right magic
		List <String> operatorTableColumnList = new Vector<String>();
		if(this.columnTableOperatorIndex.containsKey(tableColumnComposite)) {
			operatorTableColumnList = columnTableOperatorIndex.get(tableColumnComposite);
		}
		
		String tableColumnOperatorComposite = tableColumnComposite + operationName;
		if(!operatorTableColumnList.contains(tableColumnOperatorComposite)) {
			operatorTableColumnList.add(tableColumnOperatorComposite);
		}

		columnTableOperatorIndex.put(tableColumnComposite, operatorTableColumnList);

		ParamStructDetails daStruct = null;
		if(!operatorTableColumnParamIndex.containsKey(tableColumnOperatorComposite)) {
			String context = "";
			String contextPart = "";
			if(contextExpression.size() > 0) {
				context = contextExpression.pop();
				// get the context part
				contextPart = exprToTrack.printQS(exprToTrack, null) + "";
			}
			daStruct = new ParamStructDetails();
			daStruct.setColumnName(columnName);
			daStruct.setTableAlias(tableAliasName);
			daStruct.setTableName(tableName);
			daStruct.setCurrentValue(constantValue);
			daStruct.setOperator(actualOperationName);
			daStruct.setuOperator(operationName); // this is the unique operator so that it can be pegged
			daStruct.setContext(context);
			daStruct.setContextPart(contextPart);
			daStruct.setDefQuery(defQuery);

			// need to get the current select struct to add to this

			if(constantType.equalsIgnoreCase("string")) {
				daStruct.setType(PixelDataType.CONST_STRING);
				daStruct.setQuote(QUOTE.SINGLE);
			} else if(constantType.equalsIgnoreCase("double")) {
				daStruct.setType(PixelDataType.CONST_DECIMAL);
				daStruct.setQuote(QUOTE.NO);
			} else if(constantType.equalsIgnoreCase("long")) {
				daStruct.setType(PixelDataType.CONST_INT);
				daStruct.setQuote(QUOTE.NO);
			} else if(constantType.equalsIgnoreCase("date")) {
				daStruct.setType(PixelDataType.CONST_DATE);
				daStruct.setQuote(QUOTE.SINGLE);
			} else if(constantType.equalsIgnoreCase("timestamp")) {
				daStruct.setType(PixelDataType.CONST_TIMESTAMP);
				daStruct.setQuote(QUOTE.SINGLE);
			}

			operatorTableColumnParamIndex.put(tableColumnOperatorComposite, daStruct);						
			if(context.length() > 0) {
				contextExpression.push(context);
			}
		} else {
			daStruct = operatorTableColumnParamIndex.get(tableColumnOperatorComposite);
		}
		List <GenExpression> allExpressions = new Vector<GenExpression>();
		// now add this gen expression to it
		if(paramToExpressionMap.containsKey(daStruct)) {
			allExpressions = paramToExpressionMap.get(daStruct);
		}
		allExpressions.add(exprToTrack);
		paramToExpressionMap.put(daStruct, allExpressions);
		paramStringToParamMap.put(tableColumnOperatorComposite, daStruct);
		
		uniqueCounter++;
		
		//System.err.println("Parameterizing " + columnName + " with <><> " + defQuery);

		return tableColumnOperatorComposite;
	}
	
	// get all the param structs for 
	public List <ParamStructDetails> getParams()
	{
		List <ParamStructDetails> allParams = new Vector<ParamStructDetails>();
		allParams.addAll(paramToExpressionMap.keySet());
		return allParams;
	}
	
	//////////////////////////////////////////////////////////////METHOD FOR MANIPULATING PARAMETERS /////////////////////////////////////////////////////
	//////////////////////////////////////////////////////////////METHOD FOR MANIPULATING FUNCTIONS /////////////////////////////////////////////////////

	public void addFunctionExpression(String functionName, GenExpression expr)
	{
		List <GenExpression> exprList = null;
		if(functionExpressionMapper.containsKey(functionName))
			exprList = functionExpressionMapper.get(functionName);
		else
			exprList = new ArrayList <GenExpression>();
		
		if(!exprList.contains(expr))
			exprList.add(expr);
		
		functionExpressionMapper.put(functionName, exprList);
	}
	
	
	public static Object []  getLineage(GenExpression starter, String name, Map <Integer, List <GenExpression>> selectLineage, Map <Integer, GenExpression> columnLineage, List <GenExpression> allInstances, int level)
	{
		// from this point on, tries to find and print the lineage of this column i.e. which particular 
		// find the column name and table name
		// if the table name is not there then there is possibly only one table
		// go down to from
		
		//System.err.println("Processing level .. " + level);
		if(selectLineage == null)
			selectLineage = new HashMap<Integer, List<GenExpression>>();
		if(columnLineage == null)
			columnLineage = new HashMap<Integer, GenExpression>();
		
		if(allInstances == null)
			allInstances = new Vector<GenExpression>();

		GenExpression identifiedSelector = null;
		
		List <GenExpression> levelLineage = null;
		if(selectLineage.containsKey(level))
			levelLineage = selectLineage.get(level);
		else
			levelLineage = new Vector<GenExpression>();
		
		String newName = name;
		// find if this is even there as the first projection	
		if(! (starter instanceof OperationExpression)) // make sure this is not a union
		{
			for(int selectorIndex = 0;selectorIndex < starter.nselectors.size();selectorIndex++)
			{
				// check to see if the selector is here if yes, then find the from
				// if the from is not a simple from then pass to that 
				GenExpression curSelector = starter.nselectors.get(selectorIndex);
				
					
				String selectorAlias = curSelector.leftAlias;
				//System.err.println("Selector alias is set to .. " + selectorName);
				String selectorColumn = curSelector.getLeftExpr();

				// there is a possibility this is a functional expression
				if(curSelector instanceof FunctionExpression)
					selectorColumn = getColumnFromFunctionExpression((FunctionExpression)curSelector, false);
					
				// remove the quotes
				if(selectorAlias != null)
				{
					selectorAlias = selectorAlias.replace("`", "");
					selectorAlias = selectorAlias.replace("'", "");
					selectorAlias = selectorAlias.replace("\"", "");
					
					//System.err.println(selectorName + " <<>> " + name);
					
					if(name.contentEquals(selectorAlias))
					{
						allInstances.add(curSelector);
						identifiedSelector = curSelector;
						// if it is a function and the selector alias is set to null.. use the column itself
						//System.err.println("Left expression " + curSelector.getLeftExpr());
						if(curSelector instanceof FunctionExpression)
							newName = getColumnFromFunctionExpression((FunctionExpression)curSelector, false);
						else if(selectorColumn != null)
							newName = selectorColumn;
						else
							newName = selectorAlias;

						columnLineage.put(level+1, curSelector);
						
						break;
					}
				}
				// compare both
				if(name.contentEquals(selectorColumn))
				{
					allInstances.add(curSelector);
					identifiedSelector = curSelector;
					//System.err.println("Left expression " + curSelector.getLeftExpr());
					if(curSelector instanceof FunctionExpression)
						newName = getColumnFromFunctionExpression((FunctionExpression)curSelector, false);
					else if(selectorColumn != null)
						newName = selectorColumn;
					else
						newName = selectorAlias;
					
					columnLineage.put(level+1, curSelector);
					
					break;
				}
			}
		}		
		
		// groupby
		// do I care about groupby ?
		// given we are neutralizing now..
		// I think we should add the groupby as well
		// the only reason is if possibly this was not there in the selector ?
		//if(identifiedSelector == null)
		for(int groupIndex = 0;groupIndex < starter.ngroupBy.size();groupIndex++)
		{
			GenExpression curSelector = starter.ngroupBy.get(groupIndex);
			String selectorAlias = curSelector.leftAlias;
			//System.err.println("Selector alias is set to .. " + selectorName);
			String selectorColumn = curSelector.getLeftExpr();
			
			// there is a possibility this is a functional expression
			if(curSelector instanceof FunctionExpression)
				selectorColumn = getColumnFromFunctionExpression((FunctionExpression)curSelector, false);

			
			if(selectorAlias != null)
			{
				// remove the quotes
				selectorAlias = selectorAlias.replace("`", "");
				selectorAlias = selectorAlias.replace("'", "");
				selectorAlias = selectorAlias.replace("\"", "");
				
				//System.err.println(selectorName + " <<>> " + name);
				
				if(name.contentEquals(selectorAlias))
				{
					//levelLineage.add(curSelector);
					allInstances.add(curSelector);
					if(identifiedSelector == null)
					{
						identifiedSelector = curSelector;
						System.err.println("Left expression " + curSelector.getLeftExpr());
						if(curSelector instanceof FunctionExpression)
							newName = getColumnFromFunctionExpression((FunctionExpression)curSelector, false);
						else if(selectorColumn != null)
							newName = selectorColumn;
						else
							newName = selectorAlias;
						columnLineage.put(level+1, curSelector);
					}					
					break;
				}
			}
			// compare both
			if(name.contentEquals(selectorColumn) )
			{
				allInstances.add(curSelector);
				identifiedSelector = curSelector;
				//System.err.println("Left expression " + curSelector.getLeftExpr());
				if(curSelector instanceof FunctionExpression)
					newName = getColumnFromFunctionExpression((FunctionExpression)curSelector, false);
				else if(selectorColumn != null)
					newName = selectorColumn;
				else
					newName = selectorAlias;
				
				columnLineage.put(level+1, curSelector);
				
				break;
			}
			
		}

		// filters
		// add the filters
		if(starter.filter != null)
		{
			List <GenExpression> filterExpressions = getSelectorInComposite(name,  starter.filter, null);
			allInstances.addAll(filterExpressions);
		}		
		// process the from to see if it is a table or a full select
		// if this is not a composite we are all set
		// we need to now do this for each of the gen expressions
		//if(identifiedSelector != null)
		{
			if(starter.from != null && starter.from.composite)
			{
				//System.err.println("Comparing from " + starter.from);
				// ot sure I need this check
				//if(fromAlias.contentEquals(starter.tableName) && !levelLineage.contains(starter.from))
					levelLineage.add(starter.from);
			}
			//else if(!allInstances.contains(starter)) // found the bogey
			//	allInstances.add(starter);
		}
		
		// now come the joins
		//if(identifiedSelector == null)
		for(int joinIndex = 0;joinIndex < starter.joins.size();joinIndex++)
		{
			GenExpression curJoin = starter.joins.get(joinIndex);	
			List <GenExpression> allColumns = getSelectorInComposite(name, curJoin.body, null);
			// find if this join is the one
			// if the body is composite
			if(curJoin.from.composite)
				levelLineage.add(curJoin.from); // search for next level
			allInstances.addAll(allColumns);
			// the body may be composite or the body may be simple
		}
		
		// TODO: Union
		
		// also process final query
		// need to track the name at each level as well as w
		
		// add these back 
		selectLineage.put(level, levelLineage);
		//columnLineage.put(level, levelLineage);
		
		//System.out.println("--x--x--x-- >>" +  newName);
		
		// now basically run through the select lineage list
		if(levelLineage.size() > 0)
		{
			for(int nextIndex = 0;nextIndex < levelLineage.size();nextIndex++)
				return getLineage(levelLineage.get(nextIndex), newName, selectLineage, columnLineage, allInstances, (level+1));		
		}		
		
		Object [] badStruct = new Object[4];
		badStruct[0] = selectLineage;
		badStruct[1] = columnLineage;
		badStruct[2] = allInstances;
		badStruct[3] = level;
		
		return badStruct;
	}

	// known anomalies
	// #1 - When the join condition precedes the filter condition
	
	public static String getColumnFromFunctionExpression(FunctionExpression expr, boolean table)
	{
		String retString = null;
		
		if(expr.expressions.size() == 1)
		{
			GenExpression gep = expr.expressions.get(0);
			
			if(gep instanceof FunctionExpression)
				retString =  getColumnFromFunctionExpression((FunctionExpression)gep, table);

			else if(gep.operation != null && gep.operation.equalsIgnoreCase("column"))
			{
				String tableName = gep.tableName;
				retString = gep.getLeftExpr();
				if(table)
					retString = tableName + "." + retString;
			}
		}
		
		return retString;
		
	}
	
	
	public static List<GenExpression> getSelectorInComposite(String name, GenExpression qs, List <GenExpression> selectorList)
	{
		if(selectorList == null)
			selectorList = new Vector<GenExpression>();
		
		
		List <GenExpression> nextIterator = new Vector<GenExpression>();
		
		boolean neutralLeft = false;
		boolean neutralRight = false;
		
		// need to accomodate for operation
		if(qs.leftItem != null)
		{
			if(qs.leftItem instanceof GenExpression && !((GenExpression)qs.leftItem).composite && ((GenExpression)qs.leftItem).getOperation().equalsIgnoreCase("Column"))
			{
				GenExpression leftItem = (GenExpression)qs.leftItem;
				// do the left alias magic
				
				// this is where we need to do the paranthesis again I think
				String selectorName = leftItem.leftAlias;
				if(selectorName == null)
					selectorName = leftItem.getLeftExpr();
				
				if(name.contentEquals(selectorName))
				{
					//levelLineage.add(curSelector);
					selectorList.add(qs);
					neutralLeft = true;
					if(selectorList.contains((GenExpression)qs.parent.rightItem))
						selectorList.add(qs.parent);
				}
			}
			else if(qs.leftItem instanceof GenExpression)
				nextIterator.add((GenExpression)qs.leftItem);
		}
		
		if(qs.rightItem != null)
		{
			if(qs.rightItem instanceof GenExpression && !((GenExpression)qs.rightItem).composite && ((GenExpression)qs.rightItem).getOperation().equalsIgnoreCase("Column"))
			{
				GenExpression rightItem = (GenExpression)qs.rightItem;
				// do the left alias magic
				
				// this is where we need to do the paranthesis again I think
				String selectorName = rightItem.leftAlias;
				if(selectorName == null)
					selectorName = rightItem.getLeftExpr();
				
				if(name.contentEquals(selectorName))
				{
					//levelLineage.add(curSelector);
					selectorList.add(qs);
					neutralRight = true;
					if(selectorList.contains((GenExpression)qs.parent.leftItem))
						selectorList.add(qs.parent);
				}
			}
			else if(qs.rightItem instanceof GenExpression)
				nextIterator.add((GenExpression)qs.rightItem);
		}
				
		if(nextIterator.size() > 0)
		{
			for(int iterIndex = 0;iterIndex < nextIterator.size();iterIndex++)
				getSelectorInComposite(name, nextIterator.get(iterIndex), selectorList);
		}
		return selectorList;
	}
	
	
	public static String getPhysicalColumnName(GenExpression starter, String projectionName)
	{
		Object [] output = getLineage(starter, projectionName, null, null, null, 0);
		
		// get the final query
		int level = (Integer)output[3] + 1;
		
		
		Map <Integer, GenExpression> columnLineage = (Map<Integer, GenExpression>)output[1];
		
		GenExpression selector = null;
		
		// find the latest columnLineage
		do {
			selector = columnLineage.get(level);
			level--;
		}while(selector == null && level >= 0);
		
		
		if(selector != null)
		{
			if(selector instanceof FunctionExpression)
				return getColumnFromFunctionExpression((FunctionExpression)selector, true);
			else
				return selector.tableName + "." + selector.getLeftExpr();
		}
		return null;		
	}
	
	
	public static void neutralizeSelector(GenExpression starter, String projectionName, boolean neutralize)
	{
		// need to get the tree and neutralize the selector at every level
		Object [] output = getLineage(starter, projectionName, null, null, null, 0);
		
		// I just need to grab the first array and neutralize
		List <GenExpression> instanceList = (List <GenExpression>)output[2];
		for(int instanceIndex = 0;instanceIndex < instanceList.size();instanceIndex++)
		{
			GenExpression curExpression = instanceList.get(instanceIndex);
			curExpression.neutralize = neutralize;
		}		
	}
	
	

	public List <String> getColumnsForFunction(String functionName)
	{
		List <String> retList = new ArrayList <String>();
		if(functionExpressionMapper.containsKey(functionName))
		{
			List <GenExpression> allExprs = functionExpressionMapper.get(functionName);
			for(int exprIndex = 0;exprIndex < allExprs.size();exprIndex++)
			{
				GenExpression curSelector = allExprs.get(exprIndex);
				String curName = null;
				
				//if(expr instanceof FunctionExpression)
				{
					// need to do some magic here
					if(curSelector instanceof FunctionExpression)
						curName = getColumnFromFunctionExpression((FunctionExpression)curSelector,false);
					else if(curSelector.leftAlias != null)
						curName = curSelector.leftAlias;
					else
						curName = curSelector.getLeftExpr();
				}	
				System.out.println("curName is set to " + curName);
				String physicalName = getPhysicalColumnName(root, curName);
				System.out.println("Physical name "+ physicalName);
			
				if(physicalName != null)
					retList.add(physicalName);
				else
					retList.add(curName);
			}
		}
		
		return retList;
	}
	
	public void neutralizeFunction(GenExpression starter, String functionName, boolean neutralize)
	{

		// I just need to grab the first array and neutralize
		if(functionExpressionMapper.containsKey(functionName))
		{
			List <GenExpression> allExprs = functionExpressionMapper.get(functionName);
			for(int exprIndex = 0;exprIndex < allExprs.size();exprIndex++)
			{
				FunctionExpression curSelector = (FunctionExpression)allExprs.get(exprIndex);
				curSelector.neutralizeFunction = neutralize;
			}
		}
			
	}

	// add function to a selector
	public void addFunctionToSelector(GenExpression select, String selectorName, String functionName)
	{
		// get the selectors parent
		// create a function expression by setting operation to function
		// add the current selector to the expression
		// get the alias and if not available, use the selector directly
		// remove this from the list of selectors
		// set the alias for function expression to be the alias
		// add this to the selectors
		
		GenExpression selector = null;
		for(int selectIndex = 0;selectIndex < select.nselectors.size();selectIndex++) 
		{
			GenExpression curSelector = select.nselectors.get(selectIndex);
			String selectorAlias = curSelector.leftAlias;
			//System.err.println("Selector alias is set to .. " + selectorName);
			String selectorColumn = curSelector.getLeftExpr();

			// there is a possibility this is a functional expression
			if(curSelector instanceof FunctionExpression)
				selectorColumn = getColumnFromFunctionExpression((FunctionExpression)curSelector, false);

			if((selectorAlias != null && selectorName.equalsIgnoreCase(selectorAlias) )|| selectorName.equalsIgnoreCase(selectorColumn))
			{
				selector = curSelector;
				break;
			}	
		}
		
		if(selector != null)
		{
			FunctionExpression funExpression = new FunctionExpression();
			funExpression.operation = "function";
			funExpression.setExpression(functionName);
			funExpression.expressions.add(selector);
			String alias = selector.leftAlias;
			if(alias == null)
				alias = selector.getLeftExpr();
			
			funExpression.leftAlias = alias;
			select.nselectors.remove(selector);
			select.nselectors.add(funExpression);
		}		
		// done
	}
		
	// get the final query 
	public static String transformQueryWithParams(String originalQuery, List <ParamStructDetails> incomingStructs)
	{
		String retQuery = null;
		try {
			SqlParser2 parse2 = new SqlParser2();
			parse2.parameterize = true;
			
			GenExpressionWrapper wrapper = parse2.processQuery(originalQuery);
			//System.out.println("Before Transformation " + wrapper.root.printQS(wrapper.root, null) + "");
			for(int paramIndex = 0;paramIndex < incomingStructs.size();paramIndex++)
			{
				ParamStructDetails thisStruct = incomingStructs.get(paramIndex);
				String ParamStructDetailsKey = thisStruct.getParamKey();
				
				if(wrapper.operatorTableColumnParamIndex.containsKey(ParamStructDetailsKey))
				{
					ParamStructDetails targetStruct = wrapper.operatorTableColumnParamIndex.get(ParamStructDetailsKey);
					// remove this struct from the overall so it wont fill
					wrapper.paramToExpressionMap.remove(targetStruct);
				}
			}
			wrapper.fillParameters();
			retQuery = wrapper.root.printQS(wrapper.root, null) + "";
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return retQuery;
	}
	
	/**
	 * Transform the query and replace the param struct with the user defined param names via the lookup
	 * @param originalQuery
	 * @param incomingStructs
	 * @param detailsLookup
	 * @return
	 * @throws Exception
	 */
	public static String transformQueryWithParams(String originalQuery, List<ParamStructDetails> incomingStructs, Map<ParamStructDetails, ParamStruct> detailsLookup) throws Exception {
		String retQuery = null;
		SqlParser2 parse2 = new SqlParser2();
		parse2.parameterize = true;
		
		GenExpressionWrapper wrapper = parse2.processQuery(originalQuery);
		wrapper.fillParameters(incomingStructs, detailsLookup);
		retQuery = GenExpression.printQS(wrapper.root, null) + "";
		
		return retQuery;
	}
	
	public Map <String, Object>  getAllParamNames()
	{
		// gets all of the param names in a query like this
		/*
		 * SELECT  actor_name, title, gender
		  	FROM actor
		  WHERE gender  > <actor_genderand0_left>>   AND title  IN  (
		SELECT  title
		  FROM mv
		  WHERE director  = <mv_directorand1_left=>   AND revenue_domestic  > budget  )   AND actor_name  IN  (<actor_actor_namein2>) 

		 */
		
		// here the param names would be <actor_genderand0_left>> or  <mv_directorand1_left=> etc. 
		// may be also give default values
		Map <String, Object> retList = new HashMap<String, Object>();
		
		// pass through param hash and add it
		Iterator <String> paramKeys = paramStringToParamMap.keySet().iterator();
		
		while(paramKeys.hasNext())
		{
			String param1 = paramKeys.next();
			ParamStructDetails daStruct = paramStringToParamMap.get(param1);			
			//String key = 
			Object value = daStruct.getCurrentValue();
			retList.put(param1, value);
		}
		
		return retList;
	}
	
	public Object getCurrentValueOfParam(String paramName)
	{
		// gets the current value fo the set param name
		// like <actor_genderand0_left>>
		// the front end should be able to directly substitute this value
		// this is fairly straight forward
		ParamStructDetails daStruct = paramStringToParamMap.get(paramName);
		if(daStruct !=null)
		//String key = 
			return daStruct.getCurrentValue();
		return null;
	}

	public boolean setCurrentValueOfParam(String paramName, Object value)
	{
		// sets the current value fo the set param name
		// like <actor_genderand0_left>>
		// the front end should be able to directly substitute this value
		ParamStructDetails daStruct = paramStringToParamMap.get(paramName);
		if(daStruct !=null)
		{
			daStruct.setCurrentValue(value);
			return true;
		}
		return false;
	}
	
	public String getQueryForParam(String paramName)
	{
		// somehow need to construct the query struct that can be used to query the possible values
		// sets the current value fo the set param name
		// like <actor_genderand0_left>>
		// the front end should be able to directly substitute this value
		ParamStructDetails daStruct = paramStringToParamMap.get(paramName);
		if(daStruct !=null)
			return daStruct.getDefQuery();
		return null;
	}
	
	public String generateQuery(boolean validate) throws Exception
	{
		String finalQuery = ((GenExpression)root).printQS(((GenExpression)root), new StringBuffer()).toString(); 
		
		System.err.println(finalQuery);
		
		// the real test is can I parse it back :)
		if(validate)
		{
			Statement stmt = CCJSqlParserUtil.parse(finalQuery);
		}
		System.err.println("Success ");
		return finalQuery;
	}

}
