package prerna.query.parsers;

import prerna.sablecc2.om.PixelDataType;

public class ParamStruct {

	private String pixelId = null;
	public enum FILL_TYPE {MANUAL, PIXEL}
	private String tableName = null;
	private String tableAlias = null;
	private String columnName = null;
	private PixelDataType type = null;
	private Object currentValue = null;
	private Object defaultValue = null;
	private String operator = null;

	private boolean searchable = false;
	private boolean multiple = false;
	private String paramName = null;
	private String model_query = null;
	private String manualChoices = null;
	private String model_display = null; // need to turn this into an enum
	private String model_label = null; // how do you want to ask your user what to do ?
	private boolean required = false;

	private String context = null;
	private String contextPart = null;
	
	public void setPixelId(String pixelId) {
		this.pixelId = pixelId;
	}
	
	public String getPixelId() {
		return this.pixelId;
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

	public String getModel_query() {
		return model_query;
	}

	public void setModel_query(String model_query) {
		this.model_query = model_query;
	}

	public String getManualChoices() {
		return manualChoices;
	}

	public void setManualChoices(String manualChoices) {
		this.manualChoices = manualChoices;
	}

	public String getModel_display() {
		return model_display;
	}

	public void setModel_display(String model_display) {
		this.model_display = model_display;
	}

	public String getModel_label() {
		return model_label;
	}

	public void setModel_label(String model_label) {
		this.model_label = model_label;
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

	public String toString()
	{
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

}
