package prerna.rdf.engine.wrappers;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.neo4j.Neo4jEmbeddedEngine;
import prerna.om.HeadersDataRow;

public class Neo4jWrapper extends AbstractWrapper implements IRawSelectWrapper{
	private Result result;
	private Transaction transaction ;
	protected SemossDataType[] types;

	
	@Override
	public void execute() {
		GraphDatabaseService dbService = ((Neo4jEmbeddedEngine) this.engine).getGraphDatabaseService();
		try {
			transaction = dbService.beginTx();
			result = dbService.execute(this.query);
			// get columns, types
			setVariables();
			
		} finally {

		}		
	}

	private void setVariables() {
		List<String> columns = result.columns();
		headers = new String[columns.size()];
		types = new SemossDataType[columns.size()];
		for(int i=0; i < columns.size(); i++) {
			headers[i] = columns.get(i);
			//TODO get data types from cypher result set
			types[i] = SemossDataType.STRING;
		}

	}

	@Override
	public void cleanUp() {
		result.close();
		transaction.close();
	}

	@Override
	public boolean hasNext() {
		boolean next = result.hasNext();
		if(!next) {
			cleanUp();
		}
		return next;
	}

	@Override
	public IHeadersDataRow next() {
		Object[] row = new Object[headers.length];
		Map<String, Object> resultMap = result.next();
		for(int i = 0 ; i < headers.length; i++) {
			String col = headers[i];
			Object value = resultMap.get(col);
			row[i] = value;
		}
		return new HeadersDataRow(headers, row);
	}

	@Override
	public String[] getHeaders() {
		return this.headers;
	}

	@Override
	public SemossDataType[] getTypes() {
		return this.types;
	}

	@Override
	public long getNumRows() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getNumRecords() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
