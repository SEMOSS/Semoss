package prerna.rdf.engine.wrappers;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;

public class RawImpalaSelectWrapper extends RawRDBMSSelectWrapper {
	
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
			var = new String[numColumns];
			displayVar = new String[numColumns];

			for(int colIndex = 1; colIndex <= numColumns; colIndex++) {
				var[colIndex-1] = rsmd.getColumnName(colIndex);
				displayVar[colIndex-1] = rsmd.getColumnLabel(colIndex);
				//IMPALA EDITS
				//Remove the front appended math function and re-add it to address case issue due to impala returning lowercase only
				if(qs != null && !(qs instanceof HardSelectQueryStruct)) {
					if((qs.getSelectors().get(colIndex-1).getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION)){
						QueryFunctionSelector currentSelect= (QueryFunctionSelector) qs.getSelectors().get(colIndex-1);
						String aggregate = currentSelect.getFunction();
						var[colIndex-1]=var[colIndex-1].replaceFirst((aggregate.toLowerCase()), aggregate);
						displayVar[colIndex-1]=displayVar[colIndex-1].replaceFirst((aggregate.toLowerCase()), aggregate);
					}
				}
				colTypes[colIndex-1] = rsmd.getColumnType(colIndex);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
