package prerna.sablecc2.reactor.frame.r.util;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;

public class RSourceReactor extends AbstractReactor {

	public RSourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = this.keyValue.get(this.keysToGet[0]);

		AbstractRJavaTranslator rJavaTranslator = this.insight
				.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR();

		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space);

		String path = assetFolder + "/" + relativePath;
		path = path.replace('\\', '/');

		// in case your script is using other files
		// we must load in the ROOT, APP_ROOT, and USER_ROOT
		String removePathVariables = "";
		String insightRootAssignment = "";
		String appRootAssignment = "";
		String userRootAssignment = "";
	
		String insightRootPath = null;
		String appRootPath = null;
		String userRootPath = null;
		
		insightRootPath = this.insight.getInsightFolder().replace('\\', '/');
		insightRootAssignment = "ROOT <- '" + insightRootPath + "';";
		removePathVariables = "ROOT";
		
		if(this.insight.isSavedInsight()) {
			appRootPath = this.insight.getAppFolder();
			appRootPath = appRootPath.replace('\\', '/');
			appRootAssignment = "APP_ROOT <- '" + appRootPath + "';";
			removePathVariables += ", APP_ROOT";
		}
		try {
			userRootPath = AssetUtility.getAssetBasePath(this.insight, "USER");
			userRootPath = userRootPath.replace('\\', '/');
			userRootAssignment = "USER_ROOT <- '" + userRootPath + "';";
			removePathVariables += ", USER_ROOT";
		} catch(Exception ignore) {
			// ignore
		}
		
		String rScript = "with(" + rJavaTranslator.env + ", {" + insightRootAssignment + appRootAssignment + userRootAssignment + "});"; 
		rJavaTranslator.executeEmptyRDirect(rScript);
		rScript = "source(\"" + path + "\", " + rJavaTranslator.env + ");";
		rJavaTranslator.executeEmptyRDirect(rScript);
		rScript = "with(" + rJavaTranslator.env + ", { rm(" + removePathVariables + ") });"; 
		rJavaTranslator.executeEmptyRDirect(rScript);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
