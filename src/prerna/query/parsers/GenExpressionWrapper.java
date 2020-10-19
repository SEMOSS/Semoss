package prerna.query.parsers;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.query.querystruct.GenExpression;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;

public class GenExpressionWrapper {
	
	// keep table alias
	public Map<String, String> tableAlias = null;
	// keep column alias
	public  Map<String, String> columnAlias = null;
	// used to keep track of every table and column set used
	public  Map<String, Set<String>> schema = null;

	// keeps track of column to select
	public  Map <String, List<GenExpression>> columnSelect = new Hashtable<String, List<GenExpression>>();
	// keep track of table to select
	public  Map <String, List<GenExpression>> tableSelect = new Hashtable<String, List<GenExpression>>();	
	// keep a list of selects as well
	// to the columns
	public  Map <GenExpression, List<String>> selectColumns = new Hashtable<GenExpression, List<String>>();
	
	// groupby hash
	public Map <String, GenExpression> groupByHash = new HashMap<String, GenExpression>();
	public Map <String, GenExpression> joinHash = new HashMap<String, GenExpression>();

	
	public GenExpression root = null;
	
	public Map<String, List <String>> columnTableIndex = new HashMap<String, List<String>>(); // this is the highest level - acctid
	public Map<String, List <String>> columnTableOperatorIndex = new HashMap<String, List<String>>(); // this is the next highest level - clms.acctid, mbrshp.acctid
	public Map <String, ParamStruct> operatorTableColumnParamIndex = new HashMap<String, ParamStruct>(); // this is the next level - clms.acctid=, clms.acctid <
	public Map <ParamStruct, List <GenExpression>> paramToExpressionMap = new HashMap<ParamStruct, List <GenExpression>>(); // final level

	
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
		ParamStruct tableColumnOperatorParam = operatorTableColumnParamIndex.get(id);
		tableColumnOperatorParam.setCurrentValue(value);
		//replaceParameter(tableColumnOperatorParam);
	}
	
	// fills it with the latest list of parameters
	public void fillParameters()
	{
		Iterator <ParamStruct> paramIterator = paramToExpressionMap.keySet().iterator();
		while(paramIterator.hasNext())
		{
			ParamStruct daStruct = paramIterator.next();
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
	
	
	
	public void makeParameters(String columnName, Object constantValue, String operationName, String constantType, GenExpression exprToTrack, String tableName)
	{
			//String tableAliasName = columnName.substring(0, columnName.indexOf("."));
			String tableAliasName = tableName;
			if(tableAlias.containsKey(tableAliasName))
				tableName = tableAlias.get(tableAliasName);

			//columnName = columnName.replace(tableAliasName + ".", "");
			//columnName = columnName.replace(".", "");

			// add it to the column index first
			List <String> tableColumnList = new Vector<String>();
			if(this.columnTableIndex.containsKey(columnName))
				tableColumnList = columnTableIndex.get(columnName);

			String tableColumnComposite = tableName + "." + columnName;

			if(!tableColumnList.contains(tableColumnComposite))
				tableColumnList.add(tableColumnComposite);
			columnTableIndex.put(columnName, tableColumnList);
			
			// next add the operator
			List <String> operatorTableColumnList = new Vector<String>();
			if(this.columnTableOperatorIndex.containsKey(tableColumnComposite))
				operatorTableColumnList = columnTableOperatorIndex.get(tableColumnComposite);
			
			String tableColumnOperatorComposite = tableColumnComposite + operationName;
			if(!operatorTableColumnList.contains(tableColumnOperatorComposite))
				operatorTableColumnList.add(tableColumnOperatorComposite);
			
			columnTableOperatorIndex.put(tableColumnComposite, operatorTableColumnList);
			
			ParamStruct daStruct = null;
			if(!operatorTableColumnParamIndex.containsKey(tableColumnOperatorComposite))
			{
				daStruct = new ParamStruct();
				daStruct.setParamName(tableColumnOperatorComposite);
				daStruct.setColumnName(columnName);
				daStruct.tableAlias = tableAliasName;
				daStruct.setTableName(tableName);
				daStruct.setCurrentValue(constantValue);
				daStruct.operator = operationName;
				
				// need to get the current select struct to add to this
				
				if(constantType.equalsIgnoreCase("string"))
					daStruct.setType(PixelDataType.CONST_STRING);
				if(constantType.equalsIgnoreCase("double"))
					daStruct.setType(PixelDataType.CONST_DECIMAL);
				if(constantType.equalsIgnoreCase("long"))
					daStruct.setType(PixelDataType.CONST_INT);
				if(constantType.equalsIgnoreCase("date"))
					daStruct.setType(PixelDataType.CONST_DATE);
				if(constantType.equalsIgnoreCase("timestamp"))
					daStruct.setType(PixelDataType.CONST_TIMESTAMP);
				
				
				operatorTableColumnParamIndex.put(tableColumnOperatorComposite, daStruct);						
			}
			else
				daStruct = operatorTableColumnParamIndex.get(tableColumnOperatorComposite);
			
			List <GenExpression> allExpressions = new Vector<GenExpression>();
			// now add this gen expression to it
			if(paramToExpressionMap.containsKey(daStruct))
				allExpressions = paramToExpressionMap.get(daStruct);
			
			allExpressions.add(exprToTrack);
			paramToExpressionMap.put(daStruct, allExpressions);

	}
	
	// get all the param structs for 
	public List <ParamStruct> getParams()
	{
		List <ParamStruct> allParams = new Vector<ParamStruct>();
		allParams.addAll(paramToExpressionMap.keySet());
		return allParams;
	}
	
	//////////////////////////////////////////////////////////////METHOD FOR MANIPULATING PARAMETERS /////////////////////////////////////////////////////


}
