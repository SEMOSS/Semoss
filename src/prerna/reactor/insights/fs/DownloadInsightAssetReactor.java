package prerna.reactor.insights.fs;

import java.io.File;
import java.util.UUID;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.om.InsightFile;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.FileSystemUtil;
import prerna.util.Utility;

public class DownloadInsightAssetReactor extends AbstractReactor {

	public DownloadInsightAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey() };
		this.keyRequired = new int[] { 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String relativeFilePath = this.keyValue.get(this.keysToGet[0]);
		if (relativeFilePath != null) {
			relativeFilePath = Utility.normalizePath(relativeFilePath.trim());
			if (!relativeFilePath.isEmpty()) {
				relativeFilePath = relativeFilePath.replace('\\', '/');
				if (!relativeFilePath.startsWith("/")) {
					relativeFilePath = "/" + relativeFilePath;
				}
			}
		}

		String filePath = this.insight.getInsightFolder();
		if (relativeFilePath != null && !relativeFilePath.isEmpty()) {
			filePath += relativeFilePath;
		}

		File toDownloadF = new File(filePath);
		String downloadFileLocation = FileSystemUtil.prepareAssetForDownload(toDownloadF, relativeFilePath,
				this.insight.getInsightFolder());

		// store the insight file
		// in the insight so the FE can download it
		// only from the given insight
		String downloadKey = UUID.randomUUID().toString();
		InsightFile insightFile = new InsightFile();
		insightFile.setFileKey(downloadKey);
		insightFile.setFilePath(downloadFileLocation);
		insightFile.setDeleteOnInsightClose(false);
		this.insight.addExportFile(downloadKey, insightFile);
		NounMetadata retNoun = new NounMetadata(downloadKey, PixelDataType.CONST_STRING,
				PixelOperationType.FILE_DOWNLOAD);
		return retNoun;
	}

	@Override
	public String getReactorDescription() {
		return "Download a file or directory from within the insight folder";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.FILE_PATH.getKey())) {
			return "The relative file path to a file or directory within the insight folder";
		}
		return super.getDescriptionForKey(key);
	}

}