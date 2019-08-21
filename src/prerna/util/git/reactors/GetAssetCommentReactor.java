package prerna.util.git.reactors;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.git.GitRepoUtils;

public class GetAssetCommentReactor extends AbstractReactor {

	public GetAssetCommentReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// get the asset folder path
		String assetFolder = this.insight.getInsightFolder();
		assetFolder = assetFolder.replaceAll("\\\\", "/");

		// specify a file
		String filePath = this.keyValue.get(this.keysToGet[0]);

		List<Map<String, Object>> commits = GitRepoUtils.getCommits(assetFolder, filePath);

		return new NounMetadata(commits, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
	}
}
