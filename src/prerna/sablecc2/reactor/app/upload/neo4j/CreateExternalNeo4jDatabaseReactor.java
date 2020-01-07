package prerna.sablecc2.reactor.app.upload.neo4j;

import java.io.File;
import java.io.IOException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.neo4j.Neo4jEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.sablecc2.reactor.app.upload.gremlin.AbstractCreateExternalGraphReactor;

public class CreateExternalNeo4jDatabaseReactor extends AbstractCreateExternalGraphReactor {
	
	private String connectionStringKey;
	private String username;
	private String password;
	
	public CreateExternalNeo4jDatabaseReactor() {
		this.keysToGet = new String[] { 
				ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.CONNECTION_STRING_KEY.getKey(), 
				ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(), 
				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME.getKey(),
				ReactorKeysEnum.GRAPH_NAME_ID.getKey(), ReactorKeysEnum.GRAPH_METAMODEL.getKey()
				};	
	}

	@Override
	protected void validateUserInput() throws IOException {		
		this.connectionStringKey = this.keyValue.get(this.keysToGet[1]);
		if (this.connectionStringKey == null) {
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata("Could not interpret a valid Connection URL (valid example: bolt://localhost:9999)", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		// Prepend jdbc keyword for neo4j
		// TODO jdbc::neo4j needs to be a constant
		connectionStringKey = "jdbc:neo4j:" + connectionStringKey;
		
		this.username = this.keyValue.get(this.keysToGet[2]);
		if (this.username == null) {
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata("Could not interpret username", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		
		this.password = this.keyValue.get(this.keysToGet[3]);
		if (this.password == null) {
			SemossPixelException exception = new SemossPixelException(
					new NounMetadata("Could not interpret password", 
							PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
	}

	@Override
	protected File generateTempSmss(File owlFile) throws IOException {
		// the file path will become parameterized inside
		return UploadUtilities.generateTemporaryExternalNeo4jSmss(
				this.newAppId, this.newAppName, owlFile, 
				this.connectionStringKey, this.username, this.password);
	}

	@Override
	protected IEngine generateEngine() {
		Neo4jEngine neo4jEngine = new Neo4jEngine();
		neo4jEngine.setEngineId(this.newAppId);
		neo4jEngine.setEngineName(this.newAppName);
		neo4jEngine.openDB(this.smssFile.getAbsolutePath());
		return neo4jEngine;
	}

}
