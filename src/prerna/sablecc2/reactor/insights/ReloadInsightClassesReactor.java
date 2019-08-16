package prerna.sablecc2.reactor.insights;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.ReactorFactory;
import prerna.util.insight.InsightUtility;

public class ReloadInsightClassesReactor extends AbstractReactor {

	public ReloadInsightClassesReactor() {
		this.keysToGet = new String[]{"engine"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		ReactorFactory.recompile(insight.getRdbmsId());
		if(keyValue.containsKey("engine"))
			ReactorFactory.recompile(insight.getEngineId());
		return new NounMetadata("Recompile Initiated", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
	}
}
