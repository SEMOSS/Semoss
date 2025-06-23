package prerna.reactor.engine;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.*;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateEngineFilesReactor extends AbstractEngineFileReactor {

	private static final Logger classLogger = LogManager.getLogger(UpdateEngineFilesReactor.class);

	public UpdateEngineFilesReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.ENGINE.getKey(),
				ReactorKeysEnum.PAYLOAD.getKey()
		};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		String engineId = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
		validateUserAndEngineAccess(user);

		if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
			throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have access to this engine");
		}
		String enginePath = getLocalEngineBaseDirectory(engineId);

		Map<String, Object> responseData;

		try {
			responseData = updateEngineFiles(enginePath);
		} catch (IOException e) {
			classLogger.error("Error processing files", e);
			throw new RuntimeException("File processing failed: " + e.getMessage(), e);
		}

		return new NounMetadata(responseData, PixelDataType.UPLOAD_RETURN_MAP, PixelOperationType.OPERATION);
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> updateEngineFiles(String enginePath) throws IOException {
		Map<String, Object> payload = getPayload();
		File engineBaseDir = new File(enginePath);
		Set<String> currentPaths = new HashSet<>();

		writeFilesRecursively(engineBaseDir.toPath(), payload, currentPaths);
		deleteRemovedFiles(engineBaseDir, currentPaths);

		return traverseDirectory(enginePath);  // Return updated structure
	}
}
