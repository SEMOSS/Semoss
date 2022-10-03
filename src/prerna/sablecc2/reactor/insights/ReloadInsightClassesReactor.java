package prerna.sablecc2.reactor.insights;

import prerna.project.api.IProject;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.tcp.PayloadStruct;
import prerna.util.Utility;

public class ReloadInsightClassesReactor extends AbstractReactor {

	public ReloadInsightClassesReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.PROJECT.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String idToRemove = insight.getRdbmsId();
		if(this.keyValue.get(this.keysToGet[0]) != null) {
			idToRemove = this.keyValue.get(this.keysToGet[0]);
		}

		ReactorFactory.recompile(idToRemove);

		// clear the project
		if(insight.getProjectId() != null)
		{
			IProject project = Utility.getProject(insight.getProjectId());
			
			project.clearClassCache();
			if(this.insight.getUser() != null && this.insight.getUser().getTCPServer(false) != null)
			{
				PayloadStruct ps = new PayloadStruct();
				ps.operation = ps.operation.PROJECT;
				ps.projectId = insight.getProjectId();
				ps.methodName = "clearClassCache";
				ps.hasReturn = false;
				
				this.insight.getUser().getTCPServer(false).executeCommand(ps);
			}				
	
			
			//TODO:
			//TODO:
			//TODO:
			//TODO:
			//TODO:
			//TODO:
			//TODO:
			//TODO:
			//TODO:
			//TODO:
			//TODO:
	//		List<String> queriedEngines = insight.getQueriedDatabaseIds();
	//		for(int engineIndex = 0; engineIndex < queriedEngines.size(); engineIndex++) {
	//			ReactorFactory.recompile(insight.getQueriedDatabaseIds().get(engineIndex));
	//		}
		}
			return new NounMetadata("Recompile Completed", PixelDataType.CONST_STRING);
	}
}
