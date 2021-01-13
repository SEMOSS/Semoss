package prerna.query.parsers;

import java.util.Map;

import prerna.sablecc2.om.PixelDataType;

public class ParamStructDetails {

	public enum LEVEL {COLUMN, TABLE, OPERATOR, OPERATORU};
	public enum QUOTE {NO, SINGLE, DOUBLE};
	public enum BASE_QS_TYPE {SQS, HQS};
	
	private BASE_QS_TYPE baseQsType = null;
	private String appId = null;
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
	
	public BASE_QS_TYPE getBaseQsType() {
		return baseQsType;
	}

	public void setBaseQsType(BASE_QS_TYPE baseQsType) {
		this.baseQsType = baseQsType;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
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

	public String getParamKey() {
		return tableName + "_" + columnName + getuOperator() ;
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
		String appId = (String) mapInputs.get("appId");
		String pixelId = (String) mapInputs.get("pixelId");
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

		ParamStructDetails pStruct = new ParamStructDetails();
		pStruct.setBaseQsType(BASE_QS_TYPE.valueOf(baseQsType));
		pStruct.setAppId(appId);
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
		if(type != null && !type.isEmpty()) {
			pStruct.setType(PixelDataType.valueOf(type));
		}
		if(level != null && !level.isEmpty()) {
			pStruct.setLevel(LEVEL.valueOf(level));
		}
		if(quote != null && !quote.isEmpty()) {
			pStruct.setQuote(QUOTE.valueOf(quote));
		}
		
		return pStruct;
	}
	
}
