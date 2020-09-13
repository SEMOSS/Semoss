package prerna.sablecc.expressions.sql.builder;

import java.util.List;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc.expressions.IExpressionSelector;

public class SqlConstantSelector implements IExpressionSelector {
	private static final Logger LOGGER = LogManager.getLogger(SqlConstantSelector.class.getName());

	private Object value = null;
	private List<String> tableColumnsUsed = new Vector<String>();
	
	
	public SqlConstantSelector(Object value) {
		this.value = value;
	}
	
	public String toString() {
		if(value instanceof String) {
			return "'" + value + "'";
		}
		return value + "";
	}
	
	public Object getValue() {
		return this.value;
	}
	
	@Override
	public List<String> getTableColumns() {
		return tableColumnsUsed;
	}
	
	public void setTableColumnsUsed(List<String> tableColumnsUsed) {
		this.tableColumnsUsed = tableColumnsUsed;
	}

	@Override
	public String getName() {
		if(value != null) {
		return "Constant_" + value.toString().replaceAll("[^a-zA-Z0-9]", "") + "";
		}
		else {
			LOGGER.info("Constant value is NULL");
			return "Constant_" + value + "";
		}
	}

}
