package prerna.reactor.database.upload.gremlin.external;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.tinker.JanusEngine;
import prerna.reactor.database.upload.gremlin.AbstractCreateExternalGraphReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.upload.UploadInputUtility;
import prerna.util.upload.UploadUtilities;

public class CreateJanusGraphDatabaseReactor extends AbstractCreateExternalGraphReactor {

	private File file;
	private String filePath;

	public CreateJanusGraphDatabaseReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.DATABASE.getKey(), ReactorKeysEnum.FILE_PATH.getKey(),
				ReactorKeysEnum.GRAPH_TYPE_ID.getKey(), ReactorKeysEnum.GRAPH_NAME_ID.getKey(),
				ReactorKeysEnum.GRAPH_METAMODEL.getKey(), ReactorKeysEnum.USE_LABEL.getKey() };
	}

	@Override
	protected void validateUserInput() throws IOException {
		this.filePath = UploadInputUtility.getFilePath(this.store, this.insight);

		if (!(this.file = new File(this.filePath)).exists()) {
			SemossPixelException exception = new SemossPixelException(new NounMetadata("Could not find file to save.",
					PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			exception.setContinueThreadOfExecution(false);
			throw exception;
		}

		// move the file over to the correct location
		// and then update the host value
		String newLocation = this.databaseFolder.getAbsolutePath() + DIR_SEPARATOR + FilenameUtils.getName(this.file.getAbsolutePath());
		File updatedFileLoc = new File(newLocation);
		try {
			FileUtils.copyFile(this.file, updatedFileLoc);
			this.file = updatedFileLoc;
		} catch (IOException e) {
			throw new IOException("Unable to relocate uploaded file to correct database folder");
		}

	}

	@Override
	protected File generateTempSmss(File owlFile) throws IOException {
		// the file path will become parameterized inside
		return UploadUtilities.generateTemporaryJanusGraphSmss(this.newDatabaseId, this.newDatabaseName, owlFile, this.filePath,
				this.typeMap, this.nameMap, useLabel());
	}

	@Override
	protected IDatabaseEngine generateEngine() throws Exception {
		JanusEngine janusEngine = new JanusEngine();
		janusEngine.setEngineId(this.newDatabaseId);
		janusEngine.setEngineName(this.newDatabaseName);
		janusEngine.open(this.smssFile.getAbsolutePath());
		return janusEngine;
	}

}
