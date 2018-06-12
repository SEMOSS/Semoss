package prerna.util.git.reactors;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.kohsuke.github.GitHub;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitAssetMaker;
import prerna.util.git.GitUtils;

public class DeleteAssetReactor extends GitBaseReactor {

	public DeleteAssetReactor() {
		// need repository
		// Oauth
		// File name
		// Content
		this.keysToGet = new String[]{ReactorKeysEnum.REPOSITORY.getKey(), ReactorKeysEnum.FILE_NAME.getKey(), ReactorKeysEnum.CONTENT.getKey()};
	}

	@Override
	public NounMetadata execute() {

		// need to get the user
		
		organizeKeys();
		
		String token = getToken();
		// check for minmum
		checkMin(3);
		
		Logger logger = getLogger(this.getClass().getName());
		logger.info("Logging In...");
		String repoName = this.keyValue.get(this.keysToGet[0]);
		String fileName = this.keyValue.get(this.keysToGet[1]);
		
		
		GitHub ret = GitUtils.login(token);

		// create the repo.. just in case we dont have it
		// even though we get repo name.. we are not using it for create asset
		
		//GitAssetMaker.createAssetRepo(gitAccess.getAccess_token());
		GitAssetMaker.deleteAsset(token, repoName, fileName);

		if(ret == null) {
			throw new IllegalArgumentException("Could not properly login using credentials");
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.MARKET_PLACE);
	}
}
