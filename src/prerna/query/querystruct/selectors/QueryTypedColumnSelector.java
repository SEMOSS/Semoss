package prerna.query.querystruct.selectors;

import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class QueryTypedColumnSelector extends QueryColumnSelector {

//	public enum DATA_TYPE { STRING, INT, DATE, TIMESTAMP, NUMBER };
	
	protected String dataType;
	
	public QueryTypedColumnSelector() {
		super();
	}
	
	public QueryTypedColumnSelector(SemossDataType dataType) {
		super();
		this.dataType = dataType == null ? null : dataType.toString();
	}
	
	public QueryTypedColumnSelector(String qsValue) {
		super(qsValue);
	}
	
	public QueryTypedColumnSelector(String qsValue, SemossDataType dataType) {
		super(qsValue);
		this.dataType = dataType == null ? null : dataType.toString();
	}
	
	public QueryTypedColumnSelector(String qsValue, String alias) {
		super(qsValue, alias);
	}
	
	public QueryTypedColumnSelector(String qsValue, String alias, SemossDataType dataType) {
		super(qsValue, alias);
		this.dataType = dataType == null ? null : dataType.toString();
	}
	
	@Override
	public String getDataType() {
		return dataType;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}

	public void setDataType(SemossDataType dataType) {
		this.dataType = dataType == null ? null : dataType.toString();
	}
	
}
