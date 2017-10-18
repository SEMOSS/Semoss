package prerna.rdf.engine.wrappers;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import prerna.ds.r.RIterator2;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.util.ConnectionUtils;

public class RawRSelectWrapper extends AbstractWrapper implements IRawSelectWrapper {

	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	private boolean closedConnection = false;
	
	private int numColumns = 0;
	private String[] colTypeNames = null;
	private int[] colTypes = null;
	
	Iterator <IHeadersDataRow> output = null;

	IHeadersDataRow thisRow = null;

	// this is used so we do not close the engine connection
	private boolean useEngineConnection = false;

	// use this if we want to close the connection once the iterator is done
	private boolean closeConnectionAfterExecution = false;
	
	
	
	@Override
	public void execute() {
		try {
			// I also need some way to pass the name of the variable
			// so I can get the column types from it
			// actually I can just execute and give the types and headers here
			output = (Iterator<IHeadersDataRow>) engine.execQuery(query);
			RIterator2 it2 = (RIterator2)output;
			this.var = it2.getHeaders();
			this.displayVar = it2.getHeaders();
			this.colTypeNames = it2.getColTypes();
			// go through and collect the metadata around the query
		} catch (Exception e){
			e.printStackTrace();
			if(this.useEngineConnection) {
				ConnectionUtils.closeAllConnections(null, rs, stmt);
			} else {
				ConnectionUtils.closeAllConnections(conn, rs, stmt);
			}
		}
	}

	@Override
	public IHeadersDataRow next() {
		// return the row
		return output.next();
	}

	@Override
	public boolean hasNext() {
		return output.hasNext();
	}

	private void setVariables(){
	}

	@Override
	public String[] getDisplayVariables() {
		return displayVar;
	}

	@Override
	public String[] getPhysicalVariables() {
		return var;
	}
	
	@Override
	public String[] getTypes() {
		return colTypeNames;
	}
	
	@Override
	public void cleanUp() {
		// need to add this
	}
}
