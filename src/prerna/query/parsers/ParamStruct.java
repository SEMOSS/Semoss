package prerna.query.parsers;

import java.util.Map;

import prerna.sablecc2.om.PixelDataType;

public class ParamStruct {

	public enum FILL_TYPE {MANUAL, PIXEL}
	public enum LEVEL {COLUMN, TABLE, OPERATOR, OPERATORU};
	public enum QUOTE {NO, SINGLE, DOUBLE};
	
	private String pixelId = null;
	private String pixelString = null;
	
	private String tableName = null;
	private String tableAlias = null;
	private String columnName = null;
	private PixelDataType type = null;
	private Object currentValue = null;
	private Object defaultValue = null;
	private String operator = null;
	private String uOperator = null;

	private boolean searchable = false;
	private boolean multiple = false;
	private String paramName = null;
	private String modelQuery = null;
	private String manualChoices = null;
	private String modelDisplay = null; // need to turn this into an enum
	private String modelLabel = null; // how do you want to ask your user what to do ?
	private boolean required = false;

	private String context = null;
	private String contextPart = null;
	
	private FILL_TYPE fillType = null;
	private LEVEL level = null;
	private QUOTE quote = QUOTE.DOUBLE;
	
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

	public PixelDataType getType() {
		return type;
	}

	public void setType(PixelDataType type) {
		this.type = type;
	}

	public Object getCurrentValue() {
		return currentValue;
	}

	public void setCurrentValue(Object currentValue) {
		this.currentValue = currentValue;
	}

	public Object getDefaultValue() {
		return defaultValue;
	}

	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public boolean isSearchable() {
		return searchable;
	}

	public void setSearchable(boolean searchable) {
		this.searchable = searchable;
	}

	public boolean isMultiple() {
		return multiple;
	}

	public void setMultiple(boolean multiple) {
		this.multiple = multiple;
	}

	public String getParamName() {
		return paramName;
	}

	public void setParamName(String paramName) {
		this.paramName = paramName;
	}

	public String getModelQuery() {
		return modelQuery;
	}

	public void setModelQuery(String modelQuery) {
		this.modelQuery = modelQuery;
	}

	public String getManualChoices() {
		return manualChoices;
	}

	public void setManualChoices(String manualChoices) {
		this.manualChoices = manualChoices;
	}

	public String getModelDisplay() {
		return modelDisplay;
	}

	public void setModelDisplay(String modelDisplay) {
		this.modelDisplay = modelDisplay;
	}

	public String getModelLabel() {
		return modelLabel;
	}

	public void setModelLabel(String modelLabel) {
		this.modelLabel = modelLabel;
	}

	public boolean isRequired() {
		return required;
	}

	public void setRequired(boolean required) {
		this.required = required;
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

	public FILL_TYPE getFillType() {
		return fillType;
	}

	public void setFillType(FILL_TYPE fillType) {
		this.fillType = fillType;
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
	 * Generate a param struct from map inputs
	 * @param mapInputs
	 * @return
	 */
	public static ParamStruct generateParamStruct(Map<String, Object> mapInputs) {
		String pixelId = (String) mapInputs.get("pixelId");
		String pixelString = (String) mapInputs.get("pixelString");
		String tableName = (String) mapInputs.get("tableName");
		String tableAlias = (String) mapInputs.get("tableAlias");
		String columnName = (String) mapInputs.get("columnName");
		Object currentValue = mapInputs.get("currentValue");
		Object defaultValue = mapInputs.get("defaultValue");
		String operator = (String) mapInputs.get("operator");
		Boolean searchable = (Boolean) mapInputs.get("searchable");
		Boolean multiple = (Boolean) mapInputs.get("multiple");
		String paramName = (String) mapInputs.get("paramName");
		String modelQuery = (String) mapInputs.get("modelQuery");
		String manualChoices = (String) mapInputs.get("manualChoices");
		String modelDisplay = (String) mapInputs.get("modelDisplay");
		String modelLabel = (String) mapInputs.get("modelLabel");
		Boolean required = (Boolean) mapInputs.get("required");
		String context = (String) mapInputs.get("context");
		String contextPart = (String) mapInputs.get("contextPart");
		String uOperator = (String) mapInputs.get("uOperator");
		
		// these are enums
		String type = (String) mapInputs.get("type");
		String fillType = (String) mapInputs.get("fillType");
		String level = (String) mapInputs.get("level");
		String quote = (String) mapInputs.get("quote");
		
		ParamStruct pStruct = new ParamStruct();
		pStruct.setPixelId(pixelId);
		pStruct.setPixelString(pixelString);
		pStruct.setTableName(tableName);
		pStruct.setTableAlias(tableAlias);
		pStruct.setColumnName(columnName);
		pStruct.setCurrentValue(currentValue);
		pStruct.setDefaultValue(defaultValue);
		pStruct.setOperator(operator);
		if(searchable != null) {
			pStruct.setSearchable(searchable);
		}
		if(multiple != null) {
			pStruct.setMultiple(multiple);
		}
		pStruct.setParamName(paramName);
		pStruct.setModelQuery(modelQuery);
		pStruct.setManualChoices(manualChoices);
		pStruct.setModelDisplay(modelDisplay);
		pStruct.setModelLabel(modelLabel);
		if(required != null) {
			pStruct.setRequired(required);
		}
		pStruct.setContext(context);
		pStruct.setContextPart(contextPart);
		pStruct.setuOperator(uOperator);
		if(type != null && !type.isEmpty()) {
			pStruct.setType(PixelDataType.valueOf(type));
		}
		if(fillType != null && !fillType.isEmpty()) {
			pStruct.setFillType(FILL_TYPE.valueOf(fillType));
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
