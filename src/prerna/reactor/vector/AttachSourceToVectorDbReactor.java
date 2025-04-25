package prerna.reactor.vector;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IVectorDatabaseEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import java.nio.file.*;

public class AttachSourceToVectorDbReactor extends AbstractReactor {
	private static final Logger classLogger = LogManager.getLogger(AttachSourceToVectorDbReactor.class);

	public AttachSourceToVectorDbReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.FILE_NAME.getKey() };
		this.keyRequired = new int[] { 1, 1, 1 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		if (!SecurityEngineUtils.userCanEditEngine(this.insight.getUser(), engineId)) {
			throw new IllegalArgumentException("Vector db " + engineId + " does not exist or user does not have access to this engine");
		}
		IVectorDatabaseEngine vectorDatabase = Utility.getVectorDatabase(engineId);
		if (vectorDatabase == null) {
			throw new SemossPixelException("Unable to find engine");
		}
		String vectorDbPath = vectorDatabase.getDocumentsFilesPath(null);
		String fileLocation = this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		String fileName = this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey());
		String fileLocationNew = null;

		// if the file location is not defined generate a random path and set
		// location so that the front end will download

		String insightFolder = null;
		if (fileLocation != null) {
			insightFolder = this.insight.getInsightFolder();
			{
				File f = new File(Utility.normalizePath(insightFolder));
				if (!f.exists()) {
					f.mkdirs();
				}
			}
			fileLocation = fileLocation.replace("\\", "");
			fileName = fileName.replace("\\", "");
			fileLocation = insightFolder + DIR_SEPARATOR + fileName;
			fileLocationNew = insightFolder + DIR_SEPARATOR + this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey()).replace("\\", "");

		}
		if (!fileLocation.equals(fileLocationNew)) {
			Path oldSourcePath = Paths.get(fileLocationNew);
			try {
				Files.move(oldSourcePath, oldSourcePath.resolveSibling(this.keyValue.get(ReactorKeysEnum.FILE_NAME.getKey()).replace("\\", "")));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		File source = new File(fileLocation);
		File dest = new File(vectorDbPath);
		try {
			FileUtils.copyFileToDirectory(source, dest);
		} catch (IOException e) {
			throw new IllegalArgumentException("Error occurred while copying file. Detailed message = " + e.getMessage());
		}
		return null;
	}
}