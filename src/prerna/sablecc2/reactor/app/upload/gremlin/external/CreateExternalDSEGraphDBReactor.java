package prerna.sablecc2.reactor.app.upload.gremlin.external;

import java.io.File;
import java.io.IOException;

import prerna.ds.datastax.DataStaxGraphEngine;
import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.app.upload.UploadUtilities;
import prerna.sablecc2.reactor.app.upload.gremlin.AbstractCreateExternalGraphReactor;

public class CreateExternalDSEGraphDBReactor extends AbstractCreateExternalGraphReactor {

	private String host = this.keyValue.get(this.keysToGet[1]);
	private String port = this.keyValue.get(this.keysToGet[2]);
	private String username = this.keyValue.get(this.keysToGet[3]);
	private String password = this.keyValue.get(this.keysToGet[4]);
	private String graphName = this.keyValue.get(this.keysToGet[5]);
	
	public CreateExternalDSEGraphDBReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.HOST.getKey(),
				ReactorKeysEnum.PORT.getKey(), ReactorKeysEnum.USERNAME.getKey(), ReactorKeysEnum.PASSWORD.getKey(),
				ReactorKeysEnum.GRAPH_NAME.getKey(), ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME_ID.getKey(),
				ReactorKeysEnum.GRAPH_METAMODEL.getKey() };
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
		return UploadUtilities.generateTemporaryDatastaxSmss(this.newAppId, this.newAppName, 
				owlFile, this.host, this.port,
				this.username, this.password, 
				this.graphName, this.typeMap, this.nameMap);
	}

	@Override
	protected IEngine generateEngine() {
		DataStaxGraphEngine dseEngine = new DataStaxGraphEngine();
		dseEngine.setEngineId(this.newAppId);
		dseEngine.setEngineName(this.newAppName);
		dseEngine.openDB(this.smssFile.getAbsolutePath());
		return engine;
	}

}
