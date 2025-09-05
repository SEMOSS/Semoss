package prerna.reactor.security;

import java.io.File;
import java.io.IOException;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class AdminFetchRDFMapPropViewReactor extends AbstractReactor {
	
	public AdminFetchRDFMapPropViewReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		if (!SecurityAdminUtils.userIsAdmin(user)) {
			throw new IllegalArgumentException("User is not an admin and does not have access. Please login as an admin");
		}
		
		String currentRDFMapFileLoc = DIHelper.getInstance().getRDFMapFileLocation();
		File currentRDFPropFile = new File(currentRDFMapFileLoc);
		
		if(!currentRDFPropFile.exists() || !currentRDFPropFile.isFile()) {
			throw new IllegalArgumentException("Could not find RDF Map Prop file. Please reach out to an administrator for assistance");
		}
	
		StringBuilder currentRDFMapPropContent = null;
		try {
			currentRDFMapPropContent = Utility.concealRDFMapPropSensitiveInfo(currentRDFMapFileLoc);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return new NounMetadata(currentRDFMapPropContent.toString(), PixelDataType.CONST_STRING);
	}
}
