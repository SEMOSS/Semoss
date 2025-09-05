package prerna.reactor.security;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.engine.impl.SmssUtilities;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

public class GetRDFMapPropViewReactor extends AbstractReactor {
	
	public GetRDFMapPropViewReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User is not an admin and does not have access. Please login as an admin");
		}
		
				
		String currentRDFMapFileLoc = Utility.getBaseFolder() + "/RDF_MAP.prop";
		File currentRDFPropFile = new File(currentRDFMapFileLoc);
		
		if(!currentRDFPropFile.exists() || !currentRDFPropFile.isFile()) {
			throw new IllegalArgumentException("Could not find RDF Map Prop file. Please reach out to an administrator for assistance");
		}
		
		String currentRDFMapPropContent = null;
		try {
			currentRDFMapPropContent = new String(Files.readAllBytes(Paths.get(currentRDFPropFile.toURI())));
		} catch (IOException e) {
			throw new IllegalArgumentException("An error occurred reading the current RDF MAP Prop details. Detailed message = " + e.getMessage());
		}
		
		String concealedSmssContent = SmssUtilities.concealSmssSensitiveInfo(currentRDFMapPropContent);
		return new NounMetadata(concealedSmssContent, PixelDataType.CONST_STRING);
	}
}
