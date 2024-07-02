package prerna.rdf.engine.wrappers;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.Constants;


public class RawImpalaSelectWrapper extends RawRDBMSSelectWrapper {
	
	private static final Logger classLogger = LogManager.getLogger(RawImpalaSelectWrapper.class);

	private SelectQueryStruct qs;

	public RawImpalaSelectWrapper() {
		
	}
	
	public RawImpalaSelectWrapper(SelectQueryStruct qs) {
		this.qs = qs;
	}

	@Override
	protected void setVariables(){
		try {
			// get the result set metadata
			ResultSetMetaData rsmd = rs.getMetaData();
			numColumns = rsmd.getColumnCount();

			// create the arrays to store the column types,
			// the physical variable names and the display variable names
			colTypes = new int[numColumns];
			types = new SemossDataType[numColumns];
			rawHeaders = new String[numColumns];
			headers = new String[numColumns];

			for(int colIndex = 1; colIndex <= numColumns; colIndex++) {
				rawHeaders[colIndex-1] = rsmd.getColumnName(colIndex);
				headers[colIndex-1] = rsmd.getColumnLabel(colIndex);
				//IMPALA EDITS
				//Remove the front appended math function and re-add it to address case issue due to impala returning lowercase only
				if(qs != null && !(qs instanceof HardSelectQueryStruct)) {
					if((qs.getSelectors().get(colIndex-1).getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION)){
						QueryFunctionSelector currentSelect= (QueryFunctionSelector) qs.getSelectors().get(colIndex-1);
						String aggregate = currentSelect.getFunction();
						rawHeaders[colIndex-1]=rawHeaders[colIndex-1].replaceFirst((aggregate.toLowerCase()), aggregate);
						headers[colIndex-1]=headers[colIndex-1].replaceFirst((aggregate.toLowerCase()), aggregate);
					}
				}
				colTypes[colIndex-1] = rsmd.getColumnType(colIndex);
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
	}

}
