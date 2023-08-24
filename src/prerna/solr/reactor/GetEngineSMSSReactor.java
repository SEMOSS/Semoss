package prerna.solr.reactor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetEngineSMSSReactor extends AbstractReactor {

	public GetEngineSMSSReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if(!isAdmin) {
			boolean isOwner = SecurityEngineUtils.userIsOwner(user, engineId);
			if(!isOwner) {
				throw new IllegalArgumentException("Engine " + engineId + " does not exist or user does not have permissions to view the smss. User must be the owner to perform this function.");
			}
		}
		
		IEngine engine = Utility.getEngine(engineId);
		String currentSmssFileLocation = engine.getSmssFilePath();
		File currentSmssFile = new File(currentSmssFileLocation);
		
		if(!currentSmssFile.exists() || !currentSmssFile.isFile()) {
			throw new IllegalArgumentException("Could not find smss file for engine " + engineId + ". Please reach out to an administrator for assistance");
		}
		
		String currentSmssContent = null;
		try {
			currentSmssContent = new String(Files.readAllBytes(Paths.get(currentSmssFile.toURI())));
		} catch (IOException e) {
			throw new IllegalArgumentException("An error occurred reading the current engine smss details. Detailed message = " + e.getMessage());
		}
		
		String concealedSmssContent = SmssUtilities.concealSmssSensitiveInfo(currentSmssContent);
		return new NounMetadata(concealedSmssContent, PixelDataType.CONST_STRING);
	}
}
