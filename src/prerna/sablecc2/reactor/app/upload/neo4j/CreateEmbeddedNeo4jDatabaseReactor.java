package prerna.sablecc2.reactor.app.upload.neo4j;

import java.io.File;
import java.io.IOException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.neo4j.Neo4jEmbeddedEngine;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.app.upload.UploadInputUtility;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.sablecc2.reactor.app.upload.gremlin.AbstractCreateExternalGraphReactor;

public class CreateEmbeddedNeo4jDatabaseReactor extends AbstractCreateExternalGraphReactor {
	
	private String filePath;

	
	public CreateEmbeddedNeo4jDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME_ID.getKey(),
				ReactorKeysEnum.GRAPH_METAMODEL.getKey(), ReactorKeysEnum.USE_LABEL.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	protected void validateUserInput() throws IOException {
		this.filePath = UploadInputUtility.getFilePath(this.store, this.insight);
	}

	@Override
	protected File generateTempSmss(File owlFile) throws IOException {
		return UploadUtilities.generateTemporaryEmbeddedNeo4jSmss(
				this.newAppId, this.newAppName, owlFile, this.filePath, this.typeMap, this.nameMap, useLabel());	}

	@Override
	protected IEngine generateEngine() {
		Neo4jEmbeddedEngine neo4jEngine = new Neo4jEmbeddedEngine();
		neo4jEngine.setEngineId(this.newAppId);
		neo4jEngine.setEngineName(this.newAppName);
		neo4jEngine.openDB(this.smssFile.getAbsolutePath());
		return neo4jEngine;
	}
	}