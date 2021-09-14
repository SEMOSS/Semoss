package prerna.sablecc2.reactor.frame.r.util;

import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.r.AbstractRFrameReactor;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class RSourceReactor extends AbstractRFrameReactor {

	public RSourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String relativePath = Utility.normalizePath( this.keyValue.get(this.keysToGet[0])) ;

		String disable_terminal =  DIHelper.getInstance().getProperty(Constants.DISABLE_TERMINAL);
		if(disable_terminal != null && !disable_terminal.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_terminal)) {
					throw new IllegalArgumentException("Terminal and user code execution has been disabled.");
			 };
		}
		
		AbstractRJavaTranslator rJavaTranslator = this.insight
				.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR();

		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);

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
			userRootPath = AssetUtility.getAssetBasePath(this.insight, AssetUtility.USER_SPACE_KEY, false);
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
		
		List<NounMetadata> outputs = new Vector<>(1);
		outputs.add(new NounMetadata(true, PixelDataType.BOOLEAN));

		boolean smartSync = (insight.getProperty("SMART_SYNC") != null) && insight.getProperty("SMART_SYNC").equalsIgnoreCase("true");
		
		if(smartSync)
		{
			if(smartSync(rJavaTranslator))
				outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
		}

		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		//return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
