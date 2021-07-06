package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.ReactorFactory;

public class ReloadInsightClassesReactor extends AbstractReactor {

	public ReloadInsightClassesReactor() {
		this.keysToGet = new String[]{"engine"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = insight.getRdbmsId();
		if(keyValue.containsKey("engine"))
			engineId = keyValue.get("engine");
		
		ReactorFactory.recompile(engineId);
		//if(keyValue.containsKey("engine"))
		{
			for(int engineIndex = 0;engineIndex < insight.getQueriedEngines().size();engineIndex++)
				ReactorFactory.recompile(insight.getQueriedEngines().get(engineIndex));
		}
		return new NounMetadata("Recompile Initiated", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
