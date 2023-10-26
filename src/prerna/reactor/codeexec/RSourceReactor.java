package prerna.reactor.codeexec;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;

import prerna.reactor.frame.r.AbstractRFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
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
		
		//check if script sourcing terminal is disabled
		String disable_script_scource =  DIHelper.getInstance().getProperty(Constants.DISABLE_SCRIPT_SOURCE);
		if(disable_script_scource != null && !disable_script_scource.isEmpty() ) {
			 if(Boolean.parseBoolean(disable_script_scource)) {
					throw new IllegalArgumentException("Script Sourcing has been disabled.");
			 }
		}
		
		AbstractRJavaTranslator rJavaTranslator = this.insight
				.getRJavaTranslator(this.getLogger(this.getClass().getName()));
		rJavaTranslator.startR();

		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);

		String path = assetFolder + "/" + relativePath;
		path = path.replace('\\', '/');
		
		//strict script source, we will check if its .r/.R or .py/.Py
		String strict_script_source =  DIHelper.getInstance().getProperty(Constants.STRICT_SCRIPT_SOURCE);
		if(Boolean.parseBoolean(strict_script_source)){
			 String extension = FilenameUtils.getExtension(path);
			 if(!extension.equalsIgnoreCase("r")) {
					throw new IllegalArgumentException("Only user code with extensions .R or .r may be sourced by this reactor");
			 }
		}
		
		//if we have a chroot, mount the project for that user.
		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
			//get the app_root folder for the project
			this.insight.getUser().getUserMountHelper().mountFolder(assetFolder,assetFolder, false);
		}

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
		
		List<NounMetadata> outputs = new ArrayList<>(1);
		outputs.add(new NounMetadata(true, PixelDataType.BOOLEAN));

		boolean smartSync = (insight.getProperty("SMART_SYNC") != null) && insight.getProperty("SMART_SYNC").equalsIgnoreCase("true");
		if(smartSync) {
			if(smartSync(rJavaTranslator))
				outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
		}

		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
	}
}
