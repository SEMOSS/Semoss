package prerna.auth.utils.reactors.admin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class AdminGetEngineSMSSReactor extends AbstractReactor {

	public AdminGetEngineSMSSReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		User user = this.insight.getUser();
		SecurityAdminUtils adminUtils = SecurityAdminUtils.getInstance(user);
		if(adminUtils == null) {
			throw new IllegalArgumentException("User must be an admin to perform this function");
		}
		
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		if(engineId == null) {
			throw new IllegalArgumentException("Need to define the engine");
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
