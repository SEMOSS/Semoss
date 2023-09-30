//package prerna.rdf.engine.wrappers;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Result;
//import org.neo4j.graphdb.Transaction;
//
//import prerna.algorithm.api.SemossDataType;
//import prerna.engine.api.IHeadersDataRow;
//import prerna.engine.api.IRawSelectWrapper;
//import prerna.engine.impl.neo4j.Neo4jEmbeddedEngine;
//import prerna.om.HeadersDataRow;
//import prerna.util.Constants;
//
///*
// * Since neo4j-tinkerpop-api-impl is no longer supported
// * Removing logic around interacting with neo4j through gremlin
// */
//
//public class Neo4jWrapper extends AbstractWrapper implements IRawSelectWrapper {
//	
//	private static final Logger classLogger = LogManager.getLogger(Neo4jWrapper.class);
//
//	private Result result;
//	private Transaction transaction ;
//	protected SemossDataType[] types;
//	
//	@Override
//	public void execute() {
//		GraphDatabaseService dbService = ((Neo4jEmbeddedEngine) this.engine).getGraphDatabaseService();
//		transaction = dbService.beginTx();
//		result = dbService.execute(this.query);
//		// get columns, types
//		setVariables();
//	}
//
//	private void setVariables() {
//		List<String> columns = result.columns();
//		headers = new String[columns.size()];
//		types = new SemossDataType[columns.size()];
//		for(int i=0; i < columns.size(); i++) {
//			headers[i] = columns.get(i);
//			//TODO get data types from cypher result set
//			types[i] = SemossDataType.STRING;
//		}
//
//	}
//	
//	@Override
//	public void close() throws IOException {
//		result.close();
//		transaction.close();
//	}
//
//	@Override
//	public boolean hasNext() {
//		boolean next = result.hasNext();
//		if(!next) {
//			try {
//				close();
//			} catch (IOException e) {
//				classLogger.error(Constants.STACKTRACE, e);
//			}
//		}
//		return next;
//	}
//
//	@Override
//	public IHeadersDataRow next() {
//		Object[] row = new Object[headers.length];
//		Map<String, Object> resultMap = result.next();
//		for(int i = 0 ; i < headers.length; i++) {
//			String col = headers[i];
//			Object value = resultMap.get(col);
//			row[i] = value;
//		}
//		return new HeadersDataRow(headers, row);
//	}
//
//	@Override
//	public String[] getHeaders() {
//		return this.headers;
//	}
//
//	@Override
//	public SemossDataType[] getTypes() {
//		return this.types;
//	}
//
//	@Override
//	public long getNumRows() {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public long getNumRecords() {
//		// TODO Auto-generated method stub
//		return 0;
//	}
//
//	@Override
//	public void reset() {
//		// TODO Auto-generated method stub
//		
//	}
//
//	@Override
//	public boolean flushable() {
//		// TODO Auto-generated method stub
//		return false;
//	}
//	
//	@Override
//	public String flush() {
//		// TODO Auto-generated method stub
//		return null;
//	}
//}
