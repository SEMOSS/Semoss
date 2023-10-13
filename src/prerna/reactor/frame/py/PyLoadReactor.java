package prerna.reactor.frame.py;

import java.io.File;

import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PyLoadReactor extends AbstractReactor {

	public PyLoadReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FILE_PATH.getKey(), ReactorKeysEnum.SPACE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
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
		
		//pyt.runScript(name +  " = smssutil.runwrapper('smss', '" + path + "')");
		pyt.runScript(name +  " = smssutil.loadScript('smss', '" + path + "')");

		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
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
