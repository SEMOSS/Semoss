package prerna.reactor.agent.mcp;

import java.io.File;

import prerna.auth.User;
import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class MakeMCPReactor extends AbstractReactor {

	public MakeMCPReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		User user = this.insight.getUser();
		// check if user is logged in
		if (AbstractSecurityUtils.anonymousUsersEnabled() && user.isAnonymous()) {
			throwAnonymousUserError();
		}

		String projectId = this.keyValue.get(this.keysToGet[0]);
		if (!SecurityProjectUtils.userCanEditProject(user, projectId)) {
			throw new IllegalArgumentException("Project " + projectId + " does not exist or user does not have access to edit.");
		}

		String output = "unprocessed";

		String projectAssetFolder = AssetUtility.getProjectAssetsFolder(keyValue.get(keysToGet[0]));
		String pyFolderLoc = projectAssetFolder + "/py";
		File pyFolder = new File(pyFolderLoc);

		if(!pyFolder.exists() || !pyFolder.isDirectory()) {
			output = "There is no py/main.py that exists. Please create this file and then try. "
					+ "File main.py is the main driver which is utilized in terms of creating the MCP tools.";
			return new NounMetadata(output, PixelDataType.CONST_STRING);
		}

		String mcpPyFileLoc = pyFolderLoc + "/smss_driver.py";
		File mcpPyFile = new File(mcpPyFileLoc);
		if(!mcpPyFile.exists() || !mcpPyFile.isFile()) {
			output = "There is no py/main.py that exists. Please create this file and then try. "
					+ "File main.py is the main driver which is utilized in terms of creating the MCP tools.";
			return new NounMetadata(output, PixelDataType.CONST_STRING);
		
		}
		// use the smss_util to get the needed information
		String mcpFolderLoc = projectAssetFolder + "/mcp";
		File mcpFolder = new File(mcpFolderLoc);
		if(!mcpFolder.exists()) {
			mcpFolder.mkdir();
		}
		String outputFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
		mcpPyFileLoc = mcpPyFileLoc.replace("\\", "/");
		outputFileLoc = outputFileLoc.replace("\\", "/");
		String[] script = new String[] {"smssutil.gen_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc + "')"};
		output = insight.getPyTranslator().runPyAndReturnOutput(script);

		return new NounMetadata(output, PixelDataType.CONST_STRING);
	}

	@Override
	public String getReactorDescription() {
		return "Generates a mcp/py_mcp.json file from the py/main.py file function";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(ReactorKeysEnum.PROJECT.getKey())) {
			return "The unique id for the project/app";
		}
		return super.getDescriptionForKey(key);
	}
}
