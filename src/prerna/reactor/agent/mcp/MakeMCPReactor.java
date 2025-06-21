package prerna.reactor.agent.mcp;

import java.io.File;

import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

public class MakeMCPReactor extends AbstractReactor {

	// responsible for making the mcp
	// looks for project id and then makes the MCP based on it
	
	public MakeMCPReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1};
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		organizeKeys();
		
		// get the project
		// check to see if there is a py directory
		// if there is pick the main.py and ask the system to make the json
		String projectAssetFolder = AssetUtility.getProjectAssetFolder(keyValue.get(keysToGet[0]));
		String pyFolderLoc = projectAssetFolder + "/py";
		File pyFolder = new File(pyFolderLoc);
		String output = "unprocessed";
		if(pyFolder.exists() && pyFolder.isDirectory())
		{
			String mcpPyFileLoc = pyFolderLoc + "/main.py";
			File mcpPyFile = new File(mcpPyFileLoc);
			if(mcpPyFile.exists())
			{
				// use the smss_util to get the needed information
				String mcpFolderLoc = projectAssetFolder + "/mcp";
				File mcpFolder = new File(mcpFolderLoc);
				if(!mcpFolder.exists())
					mcpFolder.mkdir();
				String outputFileLoc = projectAssetFolder + "/mcp/py_mcp.json";
				mcpPyFileLoc = mcpPyFileLoc.replace("\\", "/");
				outputFileLoc = outputFileLoc.replace("\\", "/");
				String [] script = new String [] {"smssutil.gen_mcp(src_file='" + mcpPyFileLoc + "', dest_file='" + outputFileLoc + "')"};
				output = insight.getPyTranslator().runPyAndReturnOutput(script);
			}			
		}
		else
		{
			output = "There is no py/main.py that exists. Please create this file and then try. main.py is the main driver which is utilized in terms of creating the MCP tools";
		}
		
		return new NounMetadata(output, PixelDataType.CONST_STRING);
	}

}
