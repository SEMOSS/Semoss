package prerna.solr.reactor;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.impl.SmssUtilities;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

@Deprecated
public class GetDatabaseSMSSReactor extends AbstractReactor {

	public GetDatabaseSMSSReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String databaseId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		boolean isAdmin = SecurityAdminUtils.userIsAdmin(user);
		if(!isAdmin) {
			boolean isOwner = SecurityEngineUtils.userIsOwner(user, databaseId);
			if(!isOwner) {
				throw new IllegalArgumentException("Catalog " + databaseId + " does not exist or user does not have permissions to update the smss of the catalog. User must be the owner to perform this function.");
			}
		}
		
		IDatabaseEngine engine = Utility.getDatabase(databaseId);
		String currentSmssFileLocation = engine.getSmssFilePath();
		File currentSmssFile = new File(currentSmssFileLocation);
		
		if(!currentSmssFile.exists() || !currentSmssFile.isFile()) {
			throw new IllegalArgumentException("Could not find smss file for database " + databaseId + ". Please reach out to an administrator for assistance");
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
