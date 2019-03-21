package prerna.sablecc2.reactor.workspace;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class UserRootDirectoryReactor extends AbstractReactor {

	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	@Override
	public NounMetadata execute() {
		String userSpace = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + DIR_SEPARATOR + "users";
		
		String userSpaceId = null;
		if(AbstractSecurityUtils.securityEnabled()) {
			User user = this.insight.getUser();
			// TODO: figure out which user space to put you into
			AuthProvider token = user.getLogins().get(0);
			userSpaceId = token.toString() + "_" + user.getAccessToken(token).getId();
		} else {
			userSpaceId = "anonymous";
		}
		
		File userFolder = new File(userSpace + DIR_SEPARATOR + userSpaceId);
		
		File[] userFiles = userFolder.listFiles();
		int numFiles = userFiles.length;
		
		String[] fileNames = new String[numFiles];
		boolean[] isDir = new boolean[numFiles];
		for(int i = 0; i < numFiles; i++) {
			fileNames[i] = userFiles[i].getName();
			isDir[i] = userFiles[i].isDirectory();
		}
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("files", fileNames);
		retMap.put("isDir", isDir);
		retMap.put("isRoot", true);
		
		return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.USER_DIR);
	}

}
