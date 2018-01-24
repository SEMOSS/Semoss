package prerna.query.interpreters;

import java.util.Map;

import prerna.algorithm.api.SemossDataType;

public class PandasInterpreter extends AbstractQueryInterpreter {

	private String dataTableName = null;
	private Map<String, SemossDataType> colDataTypes;
	
	@Override
	public String composeQuery() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setDataTableName(String dataTableName) {
		this.dataTableName = dataTableName;
	}
}
