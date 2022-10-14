package prerna.query.parsers;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;

public class ParamStructDetails {

	public enum LEVEL {DATASOURCE, COLUMN, TABLE, OPERATOR, OPERATORU};
	public enum QUOTE {NO, SINGLE, DOUBLE};
	public enum BASE_QS_TYPE {SQS, HQS};
	public enum PARAMETER_FILL_TYPE {DATASOURCE, FILTER}

	private BASE_QS_TYPE baseQsType = BASE_QS_TYPE.SQS;
	// what type of parameter am i filling? a filter? a datasource?
	private PARAMETER_FILL_TYPE parameterFillType = PARAMETER_FILL_TYPE.FILTER;

	@Deprecated
	private String appId = null;
	private String databaseId = null;
	private String pixelId = null;
	private String pixelString = null;
	
	private String tableName = null;
	private String tableAlias = null;
	private String columnName = null;
	private Object currentValue = null;
	private String operator = null;

	private PixelDataType type = null;
	private LEVEL level = null;
	private QUOTE quote = QUOTE.DOUBLE;

	private String uOperator = null;
	private String context = null;
	private String contextPart = null;
	
	private String defQuery = null;
	
	// this is only used for saving
	// since when we optimize the recipe
	// the pixel ids get adjusted
	private transient String optimizedPixelId = null;
	// also store the import source
	// since it might not always be a Database 
	// but might be JdbcSource, FileRead, etc.
	private String importSource = null;
	
	public BASE_QS_TYPE getBaseQsType() {
		return baseQsType;
	}

	public void setBaseQsType(BASE_QS_TYPE baseQsType) {
		this.baseQsType = baseQsType;
	}

	public String getDatabaseId() {
		return databaseId;
	}

	public void setDatabaseId(String databaseId) {
		this.databaseId = databaseId;
		this.appId = databaseId;
	}

	public void setPixelId(String pixelId) {
		this.pixelId = pixelId;
	}
	
	public String getPixelId() {
		return this.pixelId;
	}
	
	public void setPixelString(String pixelString) {
		this.pixelString = pixelString;
	}
	
	public String getPixelString() {
		return this.pixelString;
	}
	
	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getTableAlias() {
		return tableAlias;
	}

	public void setTableAlias(String tableAlias) {
		this.tableAlias = tableAlias;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public Object getCurrentValue() {
		return currentValue;
	}

	public void setCurrentValue(Object currentValue) {
		this.currentValue = currentValue;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getContext() {
		return context;
	}

	public void setContext(String context) {
		this.context = context;
	}

	public String getContextPart() {
		return contextPart;
	}

	public void setContextPart(String contextPart) {
		this.contextPart = contextPart;
	}

	public String getuOperator() {
		if(uOperator == null) {
			return operator;
		}
		return uOperator;
	}

	public void setuOperator(String uOperator) {
		this.uOperator = uOperator;
	}
	
	public PixelDataType getType() {
		return type;
	}

	public void setType(PixelDataType type) {
		this.type = type;
	}
	
	public LEVEL getLevel() {
		return level;
	}

	public void setLevel(LEVEL level) {
		this.level = level;
	}
	
	public QUOTE getQuote() {
		return quote;
	}

	public void setQuote(QUOTE quote) {
		this.quote = quote;
	}
	
	public String getImportSource() {
		return importSource;
	}

	public void setImportSource(String importSource) {
		this.importSource = importSource;
	}

	public String getParamKey() {
		return tableName + "_" + columnName + getuOperator() ;
	}
	
	public String getOptimizedPixelId() {
		return optimizedPixelId;
	}

	public void setOptimizedPixelId(String optimizedPixelId) {
		this.optimizedPixelId = optimizedPixelId;
	}

	public PARAMETER_FILL_TYPE getParameterFillType() {
		return parameterFillType;
	}
	
	public void setParameterFillType(PARAMETER_FILL_TYPE parameterFillType) {
		this.parameterFillType = parameterFillType;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(tableName);
		builder.append(".");
		builder.append(columnName);
		builder.append(" ");
		builder.append(operator);
		builder.append("  ");
		builder.append(currentValue);
		builder.append("  ");
		builder.append("from ").append(tableName);

		return builder.toString();
	}
	
	/**
	 * Generate a param struct details object from map inputs
	 * @param mapInputs
	 * @return
	 */
	public static ParamStructDetails generateParamStructDetails(Map<String, Object> mapInputs) {
		String databaseId = (String) mapInputs.get("databaseId");
		if(databaseId == null) {
			databaseId = (String) mapInputs.get("appId");
		}
		String importSource = (String) mapInputs.get("importSource");
		String pixelId = (String) mapInputs.get("pixelId");
		if(pixelId == null || pixelId.isEmpty()) {
			throw new IllegalArgumentException("Must define the pixel id for the param struct");
		}
		String pixelString = (String) mapInputs.get("pixelString");
		String tableName = (String) mapInputs.get("tableName");
		String tableAlias = (String) mapInputs.get("tableAlias");
		String columnName = (String) mapInputs.get("columnName");
		Object currentValue = mapInputs.get("currentValue");
		String operator = (String) mapInputs.get("operator");

		String uOperator = (String) mapInputs.get("uOperator");
		String context = (String) mapInputs.get("context");
		String contextPart = (String) mapInputs.get("contextPart");
		
		// these are enums
		String baseQsType = (String) mapInputs.get("baseQsType");
		String type = (String) mapInputs.get("type");
		String level = (String) mapInputs.get("level");
		String quote = (String) mapInputs.get("quote");
		String parameterFillType = (String) mapInputs.get("parameterFillType");
		
		ParamStructDetails pStruct = new ParamStructDetails();
		pStruct.setDatabaseId(databaseId);
		pStruct.setImportSource(importSource);
		pStruct.setPixelId(pixelId);
		pStruct.setPixelString(pixelString);
		pStruct.setTableName(tableName);
		pStruct.setTableAlias(tableAlias);
		pStruct.setColumnName(columnName);
		pStruct.setCurrentValue(currentValue);
		pStruct.setOperator(operator);
		pStruct.setContext(context);
		pStruct.setContextPart(contextPart);
		pStruct.setuOperator(uOperator);
		if(baseQsType != null && !baseQsType.isEmpty()) {
			pStruct.setBaseQsType(BASE_QS_TYPE.valueOf(baseQsType));
		}
		if(type != null && !type.isEmpty()) {
			pStruct.setType(PixelDataType.valueOf(type));
		}
		if(level != null && !level.isEmpty()) {
			pStruct.setLevel(LEVEL.valueOf(level));
		}
		if(quote != null && !quote.isEmpty()) {
			pStruct.setQuote(QUOTE.valueOf(quote));
		}
		if(parameterFillType != null && !parameterFillType.isEmpty()) {
			pStruct.setParameterFillType(PARAMETER_FILL_TYPE.valueOf(parameterFillType));
		}
		
		return pStruct;
	}
	
	public String getFormattedValue()
	{
		// four possibilities
		// could be string
		// could be int
		// could be list of string
		// could be list of int
		StringBuffer formattedOutput = new StringBuffer("");
		
		if(currentValue instanceof List)
		{
			formattedOutput.append("[");
			List itemList = (List)currentValue;
			for(int itemIndex = 0;itemIndex < itemList.size();itemIndex++)
			{
				if(itemIndex != 0)
					formattedOutput.append(", ");
				formattedOutput.append(itemList.get(itemIndex));
			}
			formattedOutput.append("]");
		}
		else
			formattedOutput.append(currentValue);
		return formattedOutput.toString();
	}

	public String getDefQuery() {
		return defQuery;
	}

	public void setDefQuery(String defQuery) {
		this.defQuery = defQuery;
	}
	
	public void swapOptimizedIds() {
		if(this.optimizedPixelId != null) {
			String temp = this.optimizedPixelId;
			this.optimizedPixelId = this.pixelId;
			this.pixelId = temp;
		}
	}
	
	/**
	 * Get the pixel string to replace a parameter input
	 * @return
	 */
	public String getPixelStringReplacement(Object defaultValue) {
		final String FILL_QUOTE = getStringForQuote(this.quote);
		StringBuilder builder = new StringBuilder();

		if(currentValue != null) {
			// loop through results
			if(currentValue instanceof Collection) {
				Collection<Object> inputList = (Collection<Object>) currentValue;
				boolean notFirst = false;
				for(Object val : inputList) {
					if(notFirst) {
						builder.append(",");
					}
					notFirst = true;
					
					// add the value
					appendNewValue(builder, val, FILL_QUOTE);
				}
				
			} else {
				// scalar value
				appendNewValue(builder, currentValue, FILL_QUOTE);
			}
			
			// return the string builder
			return builder.toString();
		}

		// return the default value
		// same logic as scalar
		// but default value instead of scalar value
		appendNewValue(builder, defaultValue, FILL_QUOTE);
		return builder.toString();
	}
	
	/**
	 * Append a single value to the string builder for generating pixel replacement 
	 * for parameter input
	 * @param builder
	 * @param value
	 * @param FILL_QUOTE
	 */
	private void appendNewValue(final StringBuilder builder, Object value, final String FILL_QUOTE) {
		builder.append(FILL_QUOTE);
		if(this.quote == QUOTE.DOUBLE) {
			// replace existing quotes for input values
			builder.append((value + "").replace("\"", "\\\""));
		} else if(this.quote == QUOTE.SINGLE && this.baseQsType == BASE_QS_TYPE.HQS){
			builder.append((value + "").replace("'", "''"));
		} else {
			builder.append(value);
		}
		builder.append(FILL_QUOTE);
	}
	
	/**
	 * 
	 * @param quote
	 * @return
	 */
	public static String getStringForQuote(QUOTE quote) {
		if(quote == QUOTE.NO) {
			return "";
		} else if(quote == QUOTE.SINGLE) {
			return "'";
		} else if(quote == QUOTE.DOUBLE) {
			return "\"";
		}
		
		throw new IllegalArgumentException("Unknown quote type value = " + quote);
	}
}
