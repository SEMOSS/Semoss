package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class GetAssetReactor extends AbstractReactor {

	// gets a particular asset in a particular version
	// if the version is not provided - this gets the head

	public GetAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.VERSION.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// grab the version
		String version = null;
		if (this.keyValue.containsKey(ReactorKeysEnum.VERSION.getKey())) {
			version = this.keyValue.get(ReactorKeysEnum.VERSION.getKey());
		}

		// specify a file
		String asset = Utility.normalizePath(this.keyValue.get(keysToGet[0]));
		if (!asset.startsWith("/") && !asset.startsWith("\\")) {
			asset = "/" + asset;
		}

		// check if user is logged in
		String space = this.keyValue.get(this.keysToGet[2]);
		// we need to change this to asset base folder
		String assetFolder = AssetUtility.getRootFolderPath(this.insight, space, false);

		String output = null;
		if (version != null) {
			// I need a better way than output
			// probably write the file and volley the file ?
			// ideally this should be through the sym link
			output = GitRepoUtils.getFile(version, asset, assetFolder);
		} else {
			// just read the current file
			String assetFileString = assetFolder + asset;
			try {
				if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
					Path assetFilePath = Paths.get(this.insight.getUser().getUserSymlinkHelper().getUserChrootFolder(),
							assetFileString.startsWith("/") ? assetFileString.substring(1) : assetFileString);
					assetFileString = assetFilePath.toString();
				}
				output = FileUtils.readFileToString(new File(assetFileString), Charset.forName("UTF-8"));
			} catch (IOException e) {
				throw new IllegalArgumentException("Unable to read file " + asset);
			}
		}
		return new NounMetadata(output, PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
