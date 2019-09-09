package prerna.util.git.reactors;

import java.util.List;
import java.util.Vector;

import prerna.auth.User;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitRepoUtils;

public class CommitAssetReactor extends AbstractReactor {

	public CommitAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.COMMENT_KEY.getKey(), ReactorKeysEnum.IN_APP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		User user = this.insight.getUser();
		
		String comment = this.keyValue.get(this.keysToGet[1]);

		// get asset base folder to create relative path
		//boolean app = keyValue.containsKey(keysToGet[2]);
		boolean app = keyValue.containsKey(keysToGet[2]) || (keyValue.containsKey(keysToGet[0]) && keyValue.get(keysToGet[0]).startsWith("app_assets"));

		// get the asset folder path
		String assetFolder = this.insight.getInsightFolder(); 
		if(app)
			assetFolder = this.insight.getAppFolder();
		
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// get path
		String filePath = assetFolder;
		if (keyValue.containsKey(keysToGet[0])) {
			filePath = assetFolder + "/" + keyValue.get(keysToGet[0]);
			filePath = filePath.replaceAll("\\\\", "/");
		}
		
		// neutraize app_assets
		//filePath = filePath.replaceAll("app_assets", "");

		
		// add file to git
		List<String> files = new Vector<>();
		files.add(filePath);
		GitRepoUtils.addSpecificFiles(assetFolder, files);

		String author = this.insight.getUserId();
		// TODO how can I get the user email
		String email = null;
		// commit it
		GitRepoUtils.commitAddedFiles(assetFolder, comment, author, email);

		return NounMetadata.getSuccessNounMessage("Success!");
	}

}
