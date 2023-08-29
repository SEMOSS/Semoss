package prerna.query.interpreters.sql;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IDatabaseEngine;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.IQuerySelector.SELECTOR_TYPE;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class MicrosoftSqlServerInterpreter extends SqlInterpreter {

	public MicrosoftSqlServerInterpreter() {
		
	}

	public MicrosoftSqlServerInterpreter(IDatabaseEngine engine) {
		super(engine);
	}
	
	public MicrosoftSqlServerInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	@Override
	public String composeQuery() {
		if(this.qs != null && !(this.qs instanceof HardSelectQueryStruct) ) {
			if(((SelectQueryStruct) this.qs).getLimit() > 0 || ((SelectQueryStruct) this.qs).getOffset() > 0) {
				if(((SelectQueryStruct) this.qs).getCombinedOrderBy().isEmpty()) {
					// need to add an implicit order
					IQuerySelector firstSelector = this.qs.getSelectors().get(0);
					if(firstSelector.getSelectorType() == SELECTOR_TYPE.COLUMN) {
						((SelectQueryStruct) this.qs).addOrderBy(firstSelector.getQueryStructName(), "ASC");
					} else {
						((SelectQueryStruct) this.qs).addOrderBy(firstSelector.getAlias(), null, "ASC");
					}
				}
			}
		}
		// now just feed into the above
		return super.composeQuery();
	}
	
	/**
	 * Microsoft SQL Server copies the other BUT boolean fields must be in quotes
	 * @param dataType
	 * @param objects
	 * @param comparator
	 * @return
	 */
	@Override
	protected String getFormatedObject(String dataType, List<Object> objects, String comparator) {
		// this will hold the sql acceptable format of the object
		StringBuilder myObj = new StringBuilder();
		
		// defining variables for looping
		int i = 0;
		int size = objects.size();
		if(size == 0) {
			return "";
		}
		// if we can get the data type from the OWL, lets just use that
		// if we dont have it, we will do type casting...
		if(dataType != null) {
			dataType = dataType.toUpperCase();
			SemossDataType type = SemossDataType.convertStringToDataType(dataType);
			if(SemossDataType.INT == type || SemossDataType.DOUBLE == type) {
				// get the first value
				myObj.append(objects.get(0));
				i++;
				// loop through all the other values
				for(; i < size; i++) {
					myObj.append(" , ").append(objects.get(i));
				}
			} else if(SemossDataType.DATE == type || SemossDataType.TIMESTAMP == type) {
				String leftWrapper = null;
				String rightWrapper = null;
				boolean isSearch = comparator.equalsIgnoreCase(SEARCH_COMPARATOR) || comparator.equals(NOT_SEARCH_COMPARATOR);
				if(isSearch) {
					leftWrapper = "'%";
					rightWrapper = "%'";
				} else {
					leftWrapper = "\'";
					rightWrapper = "\'";
				}
				
				// get the first value
				Object val = objects.get(0);
				String d = formatDate(val, type);
				// get the first value
				myObj.append(leftWrapper).append(d).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString();
					d = formatDate(val, type);
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(d).append(rightWrapper);
				}
			} else {
				String leftWrapper = null;
				String rightWrapper = null;
				boolean isSearch = comparator.equalsIgnoreCase(SEARCH_COMPARATOR) || comparator.equals(NOT_SEARCH_COMPARATOR);
				if(isSearch) {
					leftWrapper = "'%";
					rightWrapper = "%'";
				} else {
					leftWrapper = "\'";
					rightWrapper = "\'";
				}
				
				// get the first value
				String val = AbstractSqlQueryUtil.escapeForSQLStatement(objects.get(i).toString());
				// get the first value
				if(isSearch && val.contains("\\")) {
					myObj.append(leftWrapper).append(val.replace("\\","\\\\")).append(rightWrapper);
				} else {
					myObj.append(leftWrapper).append(val).append(rightWrapper);
				}
				i++;
				for(; i < size; i++) {
					val = AbstractSqlQueryUtil.escapeForSQLStatement(objects.get(i).toString());
					// get the other values
					if(isSearch && val.contains("\\")) {
						myObj.append(" , ").append(leftWrapper).append(val.replace("\\", "\\\\")).append(rightWrapper);
					} else {
						myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
					}
				}
			}
		} 
		else {
			// do it based on type casting
			// can't have mixed types
			// so only using first value
			Object object = objects.get(0);
			if(object instanceof Number) {
				// get the first value
				myObj.append(objects.get(0));
				i++;
				// loop through all the other values
				for(; i < size; i++) {
					myObj.append(" , ").append(objects.get(i));
				}
			} else if(object instanceof java.util.Date || object instanceof java.sql.Date) {
				String leftWrapper = null;
				String rightWrapper = null;
				boolean isSearch = comparator.equalsIgnoreCase(SEARCH_COMPARATOR) || comparator.equals(NOT_SEARCH_COMPARATOR);
				if(isSearch) {
					leftWrapper = "'%";
					rightWrapper = "%'";
				} else {
					leftWrapper = "\'";
					rightWrapper = "\'";
				}
				
				// get the first value
				String val = objects.get(0).toString();
				String d = Utility.getDate(val);
				// get the first value
				myObj.append(leftWrapper).append(d).append(rightWrapper);
				i++;
				for(; i < size; i++) {
					val = objects.get(i).toString();
					d = Utility.getDate(val);
					// get the first value
					myObj.append(" , ").append(leftWrapper).append(d).append(rightWrapper);
				}
			} else {
				String leftWrapper = null;
				String rightWrapper = null;
				boolean isSearch = comparator.equalsIgnoreCase(SEARCH_COMPARATOR) || comparator.equals(NOT_SEARCH_COMPARATOR);
				if(isSearch) {
					leftWrapper = "'%";
					rightWrapper = "%'";
				} else {
					leftWrapper = "\'";
					rightWrapper = "\'";
				}
				
				// get the first value
				String val = AbstractSqlQueryUtil.escapeForSQLStatement(objects.get(i).toString());
				// get the first value
				if(isSearch && val.contains("\\")) {
					myObj.append(leftWrapper).append(val.replace("\\","\\\\")).append(rightWrapper);
				} else {
					myObj.append(leftWrapper).append(val).append(rightWrapper);
				}
				i++;
				for(; i < size; i++) {
					val = AbstractSqlQueryUtil.escapeForSQLStatement(objects.get(i).toString());
					// get the first value
					// get the other values
					if(isSearch && val.contains("\\")) {
						myObj.append(" , ").append(leftWrapper).append(val.replace("\\","\\\\")).append(rightWrapper);
					} else {
						myObj.append(" , ").append(leftWrapper).append(val).append(rightWrapper);
					}
				}
			}
		}
		
		return myObj.toString();
	}
	
}
