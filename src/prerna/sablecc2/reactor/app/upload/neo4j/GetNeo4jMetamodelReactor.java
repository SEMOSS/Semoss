package prerna.sablecc2.reactor.app.upload.neo4j;

import java.util.HashMap;
import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ConnectionUtils;
import prerna.util.GraphUtility;

public class GetNeo4jMetamodelReactor extends AbstractReactor {

	public GetNeo4jMetamodelReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(),
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String connectionStringKey = this.keyValue.get(this.keysToGet[0]);
		if (connectionStringKey == null) {
			String msg = "Requires a Connection URL (e.g. bolt://localhost:9999) to get graph metamodel";
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata(msg, PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		// Prepend jdbc keyword for neo4j
		connectionStringKey = "jdbc:neo4j:" + connectionStringKey;
		String username = this.keyValue.get(this.keysToGet[1]);
		if (username == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata(
					"Requires username to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		String password = this.keyValue.get(this.keysToGet[2]);
		if (password == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata(
					"Requires password to get graph metamodel.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		Map<String, Object> retMap = new HashMap<String, Object>();
		Connection conn = null;
		try {
			// Must specify name of Neo4j Bolt Class for JDBC to find the right
			// driver to connect to DB
			Class.forName("org.neo4j.jdbc.bolt.BoltDriver");
			// Create Connection
			conn = DriverManager.getConnection(connectionStringKey, username, password);
			// Get Metamodel
			retMap = GraphUtility.getMetamodel(conn);

		} catch (ClassNotFoundException e) {
			// If org.neo4j.jdbc.bolt.BoltDriver not found
			e.printStackTrace();
		} catch (SQLException e) {
			// From a database access error or if we called on a closed
			// connection
			e.printStackTrace();
		} finally {
			ConnectionUtils.closeConnection(conn);
		}

		return new NounMetadata(retMap, PixelDataType.MAP);
	}
}
