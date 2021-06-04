package prerna.util.git.reactors;

import java.io.File;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;

public class MakeDirectoryReactor extends AbstractReactor {

	public MakeDirectoryReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey()};
		this.keyRequired = new int[] {1, 0};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.securityEnabled()) {
			if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
				throwAnonymousUserError();
			}
		}
		
		// specify the folder from the base
		String folderName = keyValue.get(keysToGet[0]);
		String space = this.keyValue.get(this.keysToGet[1]);
		
		if(!folderName.contains("/"))
			return NounMetadata.getErrorNounMessage("You cannot create directory / files at this level");

		// if security enables, you need proper permissions
		// this takes in the insight and does a user check that the user has access to perform the operations
		String baseFolder = AssetUtility.getAssetBasePath(this.insight, space, AbstractSecurityUtils.securityEnabled());
		String folderPath = (baseFolder + "/" + folderName).replace('\\', '/');
		File folder = new File(folderPath);
		if(folder.exists() && folder.isDirectory()) {
			throw new IllegalArgumentException("Folder already exists");
		}
		folder.mkdirs();

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

}
