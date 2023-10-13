package prerna.query.querystruct;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.query.parsers.ParamStruct;
import prerna.reactor.AbstractReactor;
import prerna.reactor.insights.recipemanagement.ImportParamOptionsReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class SetParamsReactor extends AbstractReactor
{
	public SetParamsReactor()
	{
		// id _type can be column, column_table, colum_table_operator
		
		this.keysToGet = new String[] { ReactorKeysEnum.PIXEL_ID.getKey(),  ReactorKeysEnum.ID_TYPE.getKey(), ReactorKeysEnum.VALUE.getKey(),  
				ReactorKeysEnum.COLUMN.getKey(), 
				ReactorKeysEnum.TABLE.getKey(), 
				ReactorKeysEnum.OPERATOR.getKey(), 
				ReactorKeysEnum.OPERATORU.getKey()};
		this.keyRequired = new int[] {1,0,1,1, 0,0,0};
		
	}

	// execute method - GREEDY translation
	public NounMetadata execute()
	{
		organizeKeys();
		
		String pixelId = keyValue.get(keysToGet[0]);
		String type = "column"; 
		String column = keyValue.get(keysToGet[3]);
		String table = null;
		String operator = null;
		String operatorU = null;
		Object value = keyValue.get(keysToGet[2]);
		
		if(keyValue.containsKey(keysToGet[1]))
			type = keyValue.get(keysToGet[1]);
		
		if(keyValue.containsKey(keysToGet[4]))
			table = keyValue.get(keysToGet[4]);

		if(keyValue.containsKey(keysToGet[5]))
			operator = keyValue.get(keysToGet[5]);

		if(keyValue.containsKey(keysToGet[6]))
			operatorU = keyValue.get(keysToGet[6]);
		
		Object  obj = insight.getVar(ImportParamOptionsReactor.PARAM_OPTIONS);
		
		if(obj == null)
			return getError("There is no params available to modify ");
		
		Map pixelMap = (Map)obj;
		
		if(!pixelMap.containsKey(pixelId))
			return getError("No such pixel ");
		
		Map paramMap = (Map)pixelMap.get(pixelId);
		
		if(type.equalsIgnoreCase("column"))
		{
			if(column == null)
				return getError("No Column key specified ");
			updateColumn(paramMap, column, value);
		}
		
		else if(type.equalsIgnoreCase("table"))
		{
			if(column == null || table == null)
				return getError("No column or table key specified ");
			
			Map columnMap = (Map)paramMap.get(column);
			updateAllColumnTable(columnMap, table, value);
		}
		
		else if(type.equalsIgnoreCase("operator"))
		{
			if(column == null || table == null || operator == null)
				return getError("No column, table or operator key specified ");

			Map columnMap = (Map)paramMap.get(column);
			Map tableMap = (Map)columnMap.get(table);
			
			updateAllColumnTableOperator(tableMap, null, value);
		}
		
		else if(type.equalsIgnoreCase("operatoru"))
		{
			if(column == null || table == null || operator == null || operatorU == null)
				return getError("No column, table, operator or unique operator key specified ");

			Map columnMap = (Map)paramMap.get(column);
			Map tableMap = (Map)columnMap.get(table);
			Map operatorMap = (Map) tableMap.get(operator);
			
			updateAllColumnTableOperatorU(operatorMap, operatorU, value);
		}
		
		return new NounMetadata("Parameters set ", PixelDataType.CONST_STRING);
	}
	
	public void updateColumn(Map paramMap, String columnName, Object value)
	{
		if(columnName == null || !paramMap.containsKey(columnName))
			return;
		
		// work through and update the struct
		Map tableMap = (Map)paramMap.get(columnName);
		updateAllColumnTable(tableMap, null, value);
		
	}
	
	public void updateAllColumnTable(Map tableMap, String table, Object value)
	{
		if(table != null && !tableMap.containsKey(table))
			return;
		
		if(table == null)
		{
			Iterator allTables = tableMap.keySet().iterator();			
			while(allTables.hasNext())
			{
				String thisColumnTable = (String)allTables.next();
				Map columnTableOperatorMap = (Map)tableMap.get(thisColumnTable);
				// get the column map and send it
				updateAllColumnTableOperator(columnTableOperatorMap, null, value);
			}
		}
		else
		{
			// only do this
			Map columnTableOperatorMap = (Map)tableMap.get(table);
			// get the column map and send it
			updateAllColumnTableOperator(columnTableOperatorMap, null, value);
		}
	}
	
	public void updateAllColumnTableOperator(Map columnTableOperatorMap, String columnTableOperator, Object value)
	{
		if(columnTableOperator != null && !columnTableOperatorMap.containsKey(columnTableOperator))
			return;
		if(columnTableOperator == null)
		{
			Iterator allColumnTableOperators = columnTableOperatorMap.keySet().iterator();
			
			while(allColumnTableOperators.hasNext())
			{
				String thisColumnTableOperator = (String)allColumnTableOperators.next();
	
				Map columnTableOperatorUMap = (Map)columnTableOperatorMap.get(thisColumnTableOperator);
				// get the param structs and the modify
				updateAllColumnTableOperatorU(columnTableOperatorUMap, null, value);
				
			}
		}else
		{
			// do only this one
			Map columnTableOperatorUMap = (Map)columnTableOperatorMap.get(columnTableOperator);
			// get the param structs and the modify
			updateAllColumnTableOperatorU(columnTableOperatorUMap, null, value);
		}
	}
	
	public void updateAllColumnTableOperatorU(Map columnTableOperatorUMap, String columnTableOperatorU, Object value)
	{
		if(columnTableOperatorU != null && !columnTableOperatorUMap.containsKey(columnTableOperatorU))
			return;
		
		if(columnTableOperatorU == null)
		{
			// else run through all of them
			Iterator allColumnTableOperatorU = columnTableOperatorUMap.keySet().iterator();
			
			while(allColumnTableOperatorU.hasNext())
			{
				String thisColumnTableOperatorU = (String)allColumnTableOperatorU.next();
				updateParamStruct(columnTableOperatorUMap, thisColumnTableOperatorU, value);				
			}
		}
		else
		{
			updateParamStruct(columnTableOperatorUMap, columnTableOperatorU, value);
		}
	}
	
	public void updateParamStruct(Map columnTableOperatorUMap, String columnTableOperatorU, Object value)
	{		
		List <ParamStruct> curList = (List <ParamStruct>)columnTableOperatorUMap.get(columnTableOperatorU);
		for(int paramIndex = 0;paramIndex < curList.size();paramIndex++) {
			// TODO!!! THIS IS ADDING TO THE FIRST ONE EVERY TIME
			// MIGHT NEED TO REVAMP THIS!!!!
			curList.get(paramIndex).getDetailsList().get(0).setCurrentValue(value);
		}
	}
	
}