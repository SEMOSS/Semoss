package prerna.engine.api.iterator;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import prerna.algorithm.api.SemossDataType;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryFunctionSelector;
import prerna.util.ConnectionUtils;

public class ImpalaDatasourceIterator extends RdbmsDatasourceIterator {

	private SelectQueryStruct qs;
	
	public ImpalaDatasourceIterator(Connection conn) {
		super(conn);
	}
	
	public ImpalaDatasourceIterator(Connection conn, SelectQueryStruct qs) {
		super(conn);
		this.qs = qs;
	}

	@Override
	public void execute() {
		try {
			this.stmt = this.conn.createStatement();
			this.rs = this.stmt.executeQuery(this.query);

			// get the result set metadata
			ResultSetMetaData rsmd = rs.getMetaData();
			this.numColumns = rsmd.getColumnCount();

			// create the arrays to store the column types,
			// the physical variable names and the display variable names
			this.colTypes = new int[this.numColumns];
			this.types = new SemossDataType[this.numColumns];
			this.rawHeaders = new String[this.numColumns];
			this.headers = new String[this.numColumns];

			for(int colIndex = 1; colIndex <= this.numColumns; colIndex++) {
				this.rawHeaders[colIndex-1] = rsmd.getColumnName(colIndex);
				this.headers[colIndex-1] = rsmd.getColumnLabel(colIndex);
				//IMPALA EDITS
				//Remove the front appended math function and re-add it to address case issue due to impala returning lowercase only
				if(qs != null && !(qs instanceof HardSelectQueryStruct)) {
					if((qs.getSelectors().get(colIndex-1).getSelectorType() == IQuerySelector.SELECTOR_TYPE.FUNCTION)){
						QueryFunctionSelector currentSelect= (QueryFunctionSelector) qs.getSelectors().get(colIndex-1);
						String aggregate = currentSelect.getFunction();
						this.rawHeaders[colIndex-1] = this.rawHeaders[colIndex-1].replaceFirst((aggregate.toLowerCase()), aggregate);
						this.headers[colIndex-1] = this.headers[colIndex-1].replaceFirst((aggregate.toLowerCase()), aggregate);
					}
				}
				this.colTypes[colIndex-1] = rsmd.getColumnType(colIndex);
				this.types[colIndex-1] = SemossDataType.convertStringToDataType(rsmd.getColumnTypeName(colIndex));
			}
		} catch (SQLException e) {
			e.printStackTrace();
			if(this.closeConnectionAfterExecution) {
				ConnectionUtils.closeAllConnections(conn, rs, stmt);
			} else {
				ConnectionUtils.closeAllConnections(null, rs, stmt);
			}
		}
	}
}
