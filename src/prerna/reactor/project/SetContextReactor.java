package prerna.reactor.project;

import prerna.auth.User;
import prerna.ds.py.PyTranslator;
import prerna.ds.py.PyUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class SetContextReactor extends AbstractReactor {

	private static final String CLASS_NAME = SetContextReactor.class.getName();

	// takes in a the name and engine and mounts the engine assets as that variable
	// name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then
	// later, they would like to use a different mapping

	public SetContextReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), "loadPath" };
		this.keyRequired = new int[] { 1, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String context = keyValue.get(keysToGet[0]);
		if (context == null || (context=context.trim()).isEmpty()) {
			return getError("Must pass in a valid project id for the context value");
		}
		boolean load = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[1]) + "");
		

		// need to replace the app with the
		boolean success = this.insight.setContext(context);
		// attempt once to directly map it with same name
		if (!success) {
			return getError("User does not have access to set the context to " + context);
		}

		// if we have a chroot, mount the project for that user.
		if (Boolean.parseBoolean(Utility.getDIHelperProperty(Constants.CHROOT_ENABLE))) {
			// get the app_root folder for the project
			String projectAppRootFolder = AssetUtility.getProjectAppRootFolder(context);
			this.insight.getUser().getUserSymlinkHelper().symlinkFolder(projectAppRootFolder);
		}

		// if python enabled
		// set the path
		if (PyUtils.pyEnabled()) {
			String projectId = context;
			String projectAssetFolder = AssetUtility.getProjectAssetsFolder(projectId);
			projectAssetFolder = projectAssetFolder.replace("\\", "/");

			String pyFolderLoc = projectAssetFolder + "/py";

			
			// do a couple of things
			String sysImport = "import sys";
			String getpath = "print(sys.path)";
			String setPyPath = "sys.path.insert(0,'" + pyFolderLoc + "')";
			String setAssetsPath = "sys.path.insert(0,'" + projectAssetFolder + "')";
			String chDir = "os.chdir('"+ projectAssetFolder + "')";
			

			PyTranslator pyT = null;
			User user = insight.getUser();
			if (user != null) {
				pyT = user.getPyTranslator(false);
			}
			if (pyT == null && user != null && load) {
				pyT = user.getPyTranslator(true);
			} else if (load) {
				pyT = insight.getPyTranslator();
			}

			if (pyT != null) {
				// check if the path is there
				// if not insert it
				String curPath = pyT.runPyAndReturnOutput(sysImport, getpath);
				curPath = curPath.replace("\\", "/");
				if(!curPath.contains(pyFolderLoc))
					pyT.runPyAndReturnOutput(setPyPath, setAssetsPath);
			}
		}

		return new NounMetadata("Successfully set context to '" + context, PixelDataType.CONST_STRING,
				PixelOperationType.OPERATION);
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equalsIgnoreCase(this.keysToGet[1])) {
			return "Boolean if the path of the project should be loaded into the users process";
		}
		return super.getDescriptionForKey(key);
	}

}
