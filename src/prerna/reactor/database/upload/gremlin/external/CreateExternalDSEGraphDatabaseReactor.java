package prerna.reactor.database.upload.gremlin.external;

import java.io.File;
import java.io.IOException;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.datastax.DataStaxGraphEngine;
import prerna.reactor.database.upload.gremlin.AbstractCreateExternalGraphReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadUtilities;

public class CreateExternalDSEGraphDatabaseReactor extends AbstractCreateExternalGraphReactor {

	private String host = this.keyValue.get(this.keysToGet[1]);
	private String port = this.keyValue.get(this.keysToGet[2]);
	private String username = this.keyValue.get(this.keysToGet[3]);
	private String password = this.keyValue.get(this.keysToGet[4]);
	private String graphName = this.keyValue.get(this.keysToGet[5]);
	
	public CreateExternalDSEGraphDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.HOST.getKey(),
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
				ReactorKeysEnum.GRAPH_NAME.getKey(), ReactorKeysEnum.GRAPH_TYPE_ID.getKey(),
				ReactorKeysEnum.GRAPH_NAME_ID.getKey(), ReactorKeysEnum.GRAPH_METAMODEL.getKey(),
				ReactorKeysEnum.USE_LABEL.getKey() };
	}

	@Override
	protected void validateUserInput() {
		this.host = this.keyValue.get(this.keysToGet[1]);
		if (this.host == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires host to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		this.port = this.keyValue.get(this.keysToGet[2]);
		if (this.port == null) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Requires port to save.", PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}
		this.username = this.keyValue.get(this.keysToGet[3]);
		this.password = this.keyValue.get(this.keysToGet[4]);
		this.graphName = this.keyValue.get(this.keysToGet[5]);
		
	}

	@Override
	protected File generateTempSmss(File owlFile) throws IOException {
		return UploadUtilities.generateTemporaryDatastaxSmss(this.newDatabaseId, this.newDatabaseName, owlFile, this.host,
				this.port, this.username, this.password, this.graphName, this.typeMap, this.nameMap, useLabel());
	}

	@Override
	protected IDatabaseEngine generateEngine() throws Exception {
		DataStaxGraphEngine dseEngine = new DataStaxGraphEngine();
		dseEngine.setEngineId(this.newDatabaseId);
		dseEngine.setEngineName(this.newDatabaseName);
		dseEngine.open(this.smssFile.getAbsolutePath());
		return dseEngine;
	}

	public String getName()
	{
		return "CreateExternalDSEGraphDatabase";
	}

}
