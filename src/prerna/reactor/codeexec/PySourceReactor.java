package prerna.reactor.codeexec;

import java.io.File;
import java.util.List;
import java.util.Vector;

import org.apache.commons.io.FilenameUtils;

import prerna.ds.py.PyTranslator;
import prerna.reactor.frame.py.AbstractPyFrameReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PySourceReactor extends AbstractPyFrameReactor {

	public PySourceReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		
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
		
		
		String relativePath =  Utility.normalizePath( this.keyValue.get(this.keysToGet[0]));
		String path = getBaseFolder() + "/Py/" + relativePath;
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);

		// if the file is not there try in the insight
		// if(!file.exists())
		path = assetFolder + "/" + relativePath;
		path = path.replace("\\", "/");
		
		//strict script source, we will check if its .r/.R or .py/.Py
		String strict_script_source =  DIHelper.getInstance().getProperty(Constants.STRICT_SCRIPT_SOURCE);
		if(Boolean.parseBoolean(strict_script_source)){
			 String extension = FilenameUtils.getExtension(path);
			 if(!extension.equalsIgnoreCase("Py")) {
					throw new IllegalArgumentException("Only user code with extensions .py or .PY may be sourced by this reactor");
			 }
		}

		//if we have a chroot, mount the project for that user.
		if (Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.CHROOT_ENABLE))) {
			//get the app_root folder for the project
			this.insight.getUser().getUserMountHelper().mountFolder(assetFolder,assetFolder, false);
		}
		
		File file = new File(path);
		String name = file.getName();
		name = name.replaceAll(".py", "");

		String assetOutput = assetFolder + "/" +  name + ".output";
		
		PyTranslator pyt = this.insight.getPyTranslator();

		System.err.println("Hello");
		//pyt.runScript("smssutil.runwrapper(" +  path + ", " + assetOutput + ", " + assetOutput + "globals()\")");
		//pyt.runScript(name +  " = smssutil.loadScript('smss', '" + path + "')");
		
		pyt.runScript(this.insight.getUser().getVarMap(), "smssutil.runwrapper('" +  path + "', '" + assetOutput + "', '" + assetOutput + "', globals())", this.insight);
		
		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
		outputs.add(new NounMetadata(true, PixelDataType.BOOLEAN));

		boolean smartSync = (insight.getProperty("SMART_SYNC") != null) && insight.getProperty("SMART_SYNC").equalsIgnoreCase("true");
		if(smartSync)
		{
			// if this returns true
			if(smartSync(pyt))
				outputs.add(new NounMetadata(this.insight.getCurFrame(), PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE));
		}

		return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		//return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	/**
	 * Get the base folder
	 * 
	 * @return
	 */
	protected String getBaseFolder() {
		String baseFolder = null;
		try {
			baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		} catch (Exception ignored) {
			// logger.info("No BaseFolder detected... most likely running as
			// test...");
		}
		return baseFolder;
	}
}
