package prerna.util.git.reactors;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.util.Base64;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;

public class GetImageAssetReactor extends AbstractReactor {

	// gets a particular asset in a particular version
	// if the version is not provided - this gets the head

	public GetImageAssetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.VERSION.getKey(),
				ReactorKeysEnum.SPACE.getKey() };
		this.keyRequired = new int[] { 1, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		// check if user is logged in
		String space = this.keyValue.get(this.keysToGet[2]);
		String assetFolder = AssetUtility.getAssetVersionBasePath(this.insight, space, false);

		// relative path is used for git if the insight is saved
		String assetDir = "";
		if(space == null || AssetUtility.INSIGHT_SPACE_KEY.equalsIgnoreCase(space)) {
			if (this.insight.isSavedInsight()) {
				assetDir = AssetUtility.getAssetRelativePath(this.insight, space);
			} else {
				// get temp insight folder
				assetFolder = this.insight.getInsightFolder();
			}
		} else {
			// user space + app holds assets in assets folder
			assetDir = "assets";
		}

		// specify a file
		String asset = Utility.normalizePath(keyValue.get(keysToGet[0]));
		// grab the version
		String version = null;
		if (keyValue.containsKey(keysToGet[1])) {
			version = keyValue.get(keysToGet[1]);
		}

		// I need a better way than output
		// probably write the file and volley the file ?
		byte [] output = GitRepoUtils.getBinary(version, assetDir + "/" + asset, assetFolder);
		// try to see if I can convert to a string data URI and give it
		StringWriter sw = new StringWriter();
		String encodedString = Base64.getEncoder().encodeToString(output);
		String mimeType = "image/png";
		try {
			mimeType = Files.probeContentType(new File(assetFolder + "/" + assetDir + "/" + asset).toPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//sw.write("<html><body>");
		sw.write("<img src='data:" + mimeType + ";base64," + encodedString + "'>");
		//pw.write("<img src='data:image/svg+xml;base64," + encodedString + "'>");
		//sw.write("</body></html>");

		return new NounMetadata(sw.toString(), PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}

}
