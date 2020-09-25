package prerna.query.parsers;

import prerna.sablecc2.om.PixelDataType;

public class ParamStruct {

	public enum FILL_TYPE {MANUAL, PIXEL}
	String tableName = null;
	String tableAlias = null;
	String columnName = null;
	PixelDataType type = null;
	Object currentValue = null;
	Object defaultValue = null;
	String operator = null;
	
	boolean searchable = false;
	boolean multiple = false;
	String paramName = null;
	String model_query = null;
	String manualChoices = null;
	String model_display = null; // need to turn this into an enum
	String model_label = null; // how do you want to ask your user what to do ?
	boolean required = false;
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
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
		if(currentValue != null)
			return currentValue;
		return defaultValue;
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
	public boolean isMultiple() {
		return multiple;
	}
	public void setMultiple(boolean multi) {
		this.multiple = multi;
	}
	public String getPixel() {
		return model_query;
	}
	public void setPixel(String pixel) {
		this.model_query = pixel;
	}
	public String getManualChoices() {
		return manualChoices;
	}
	public void setManualChoices(String manualChoices) {
		this.manualChoices = manualChoices;
	};
	
	public void setParamName(String paramName)
	{
		this.paramName = paramName;
	}
	
	public String getParamName()
	{
		return this.paramName;
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
