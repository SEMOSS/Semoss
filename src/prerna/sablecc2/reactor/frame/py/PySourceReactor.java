package prerna.sablecc2.reactor.frame.py;

import java.io.File;
import java.util.List;
import java.util.Vector;

import prerna.ds.py.PyTranslator;
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
					throw new IllegalArgumentException("Terminal and user code execution has been disbled.");
			 };
		}
		
		String relativePath =  Utility.normalizePath( this.keyValue.get(this.keysToGet[0]));
		String path = getBaseFolder() + "/Py/" + relativePath;
		String space = this.keyValue.get(this.keysToGet[1]);
		String assetFolder = AssetUtility.getAssetBasePath(this.insight, space, false);

		// if the file is not there try in the insight
		// if(!file.exists())
		path = assetFolder + "/" + relativePath;
		path = path.replace("\\", "/");

		File file = new File(path);
		String name = file.getName();
		name = name.replaceAll(".py", "");

		String assetOutput = assetFolder + "/" +  name + ".output";
		
		PyTranslator pyt = this.insight.getPyTranslator();

		System.err.println("Hello");
		//pyt.runScript("smssutil.runwrapper(" +  path + ", " + assetOutput + ", " + assetOutput + "globals()\")");
		//pyt.runScript(name +  " = smssutil.loadScript('smss', '" + path + "')");
		
		pyt.runScript(this.insight.getUser().getVarMap(), "smssutil.runwrapper('" +  path + "', '" + assetOutput + "', '" + assetOutput + "', globals())");
		
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
