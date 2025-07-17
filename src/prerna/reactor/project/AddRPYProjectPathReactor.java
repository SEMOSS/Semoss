package prerna.reactor.project;

import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class AddRPYProjectPathReactor extends AbstractReactor {
	
	public AddRPYProjectPathReactor() {
		this.keysToGet = new String [] {ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		
		PyTranslator pyt = this.insight.getPyTranslator();
		AbstractRJavaTranslator rt = this.insight.getRJavaTranslator(this.getClass().getName());

		String projectId = keyValue.get(keysToGet[0]);
		String basePath = AssetUtility.getProjectAssetsFolder(projectId);
		String folderName = basePath + "/py";
		folderName = folderName.replace("\\", "/");


		if(pyt != null)
		{	
			pyt.runScript("import sys", "sys.path.append('" + folderName +"')");
		}
		if(rt != null)
		{
			rt.runR("setwd('" + folderName + "')");
		}
		
		return NounMetadata.getSuccessNounMessage("Added " + projectId + " to path");
	}
	
	@Override
	public String getReactorDescription() {
		return "Add the project assets folder to the python sys.path and/or the R setwd";
	}
}
