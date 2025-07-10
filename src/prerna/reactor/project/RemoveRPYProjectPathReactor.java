package prerna.reactor.project;

import prerna.ds.py.PyTranslator;
import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;

public class RemoveRPYProjectPathReactor extends AbstractReactor {
	
	public RemoveRPYProjectPathReactor() {
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
			pyt.runScript("import sys", "sys.path.remove('" + folderName +"')");
		}
		if(rt != null)
		{
			rt.runR("setwd('" + "NA" + "')");
		}
		
		return NounMetadata.getSuccessNounMessage("Removed " + projectId + " to path");
	}
	
	@Override
	public String getReactorDescription() {
		return "Remove the project assets folder from the python sys.path and/or the R setwd";
	}
}
