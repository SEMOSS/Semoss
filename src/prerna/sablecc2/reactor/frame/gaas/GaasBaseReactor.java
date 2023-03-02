package prerna.sablecc2.reactor.frame.gaas;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;

public abstract class GaasBaseReactor extends AbstractReactor {

	public String getProjectId()
	{
		String projectId = null;
		
		GenRowStruct grs = this.store.getNoun(ReactorKeysEnum.PROJECT.getKey());
		
		if(grs != null && !grs.isEmpty())
			projectId = grs.get(0).toString();
		else
		{
			projectId = this.insight.getProjectId();
			if(projectId == null)
				projectId = this.insight.getContextProjectId();
		}

		return projectId;
	}
	
}
