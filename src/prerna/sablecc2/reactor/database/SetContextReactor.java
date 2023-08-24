package prerna.sablecc2.reactor.database;

import prerna.auth.User;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;

public class SetContextReactor extends AbstractReactor {
	
	private static final String CLASS_NAME = SetContextReactor.class.getName();
	
	// takes in a the name and engine and mounts the engine assets as that variable name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then later, they would like to use a different mapping

	public SetContextReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.CONTEXT.getKey(), "loadPath"};
		this.keyRequired = new int[]{1, 0};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String context = keyValue.get(keysToGet[0]);
		boolean load = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1])+"");
		// all of this can be moved into the context reactor
		if(context == null) {
			return getError("No context is set - please use Context(<mount point>) to set context");
		}
		
		// need to replace the app with the 
		boolean success = this.insight.setContext(context);
		// attempt once to directly map it with same name
		if(!success) {
			return getError("No mount point " + context + " - please use Mount(<mount point>, App Name) to set a mount point");
		}
		
		// if python enabled
		// set the path
		if(PyUtils.pyEnabled()) {
			String assetsDir = AssetUtility.getProjectAssetFolder(context).replace("\\", "/");
			String script = "import sys\n"+
				"import os\n"+
				"sys.path.append('"+assetsDir+"')\n"+
				"os.chdir('"+assetsDir+"')"
				;
			
			PyTranslator pyT = null;
			User user = insight.getUser();
			if(user != null) {
				pyT = user.getPyTranslator(false);
			}
			if(pyT == null && user != null && load) {
				pyT = user.getPyTranslator(true);
			} else if(load) {
				pyT = insight.getPyTranslator();
			}
			
			if(pyT != null) {
				pyT.runEmptyPy(script);
			}
		}
		
		return new NounMetadata("Successfully set context to '" + context , PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equalsIgnoreCase(this.keysToGet[1])) {
			return "Boolean if the path of the project should be loaded into the users process";
		}
		return super.getDescriptionForKey(key);
	}

}
