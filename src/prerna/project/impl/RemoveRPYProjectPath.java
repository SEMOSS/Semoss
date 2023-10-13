package prerna.project.impl;

import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class RemoveRPYProjectPath extends AbstractReactor {
	
	public RemoveRPYProjectPath()
	{
		this.keysToGet = new String [] {ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() 
	{
		// TODO Auto-generated method stub
		// get the project id
		// set it as part of path
		organizeKeys();
		
		PyTranslator pyt = this.insight.getPyTranslator();
		AbstractRJavaTranslator rt = this.insight.getRJavaTranslator(this.getClass().getName());

		String projectId = keyValue.get(keysToGet[0]);
		String basePath = AssetUtility.getProjectAssetFolder(projectId);
		String folderName = basePath + "/py";
		folderName = folderName.replace("\\", "/");


		if(pyt != null)
		{	
			String path = "import sys";
			pyt.runScript(path);
			path = "sys.path.remove('" + folderName +"')";
			pyt.runScript(path);
		}
		if(rt != null)
		{
			rt.runR("setwd('" + "NA" + "')");
		}
		
		return NounMetadata.getSuccessNounMessage("Removed " + projectId + " to path");
	}
}
