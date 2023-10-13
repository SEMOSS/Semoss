package prerna.reactor.insights;

import java.util.Map;

import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class LoadInsightReactor extends OpenInsightReactor {
	
	public LoadInsightReactor() {
		this.keysToGet = new String[]{
				ReactorKeysEnum.PROJECT.getKey(), 
				ReactorKeysEnum.ID.getKey(), 
				ReactorKeysEnum.PARAM_KEY.getKey(), 
				ReactorKeysEnum.ADDITIONAL_PIXELS.getKey(),
				CACHEABLE};
	}

	@Override
	public NounMetadata execute() {
		// i want to just run the open insight
		// and then i want to shift its insight id to be this insight
		// and update the insight store to replace to the one in the runner
		NounMetadata noun = super.execute();
		Map<String, Object> runnerMap = (Map<String, Object>) noun.getValue();
		PixelRunner runner = (PixelRunner) runnerMap.get("runner");
		
		Insight in = runner.getInsight();
		// remove the current insight id from the insight store + session store
		InsightStore.getInstance().remove(in.getInsightId());
		InsightStore.getInstance().removeFromSessionHash(getSessionId(), in.getInsightId());
		
		// reset the insight id and put in store
		in.setInsightId(this.insight.getInsightId());
		InsightStore.getInstance().put(in);
		
		// return the original noun from open insight
		noun.getOpType().clear();
		noun.addAdditionalOpTypes(PixelOperationType.LOAD_INSIGHT);
		return noun;
	}
}