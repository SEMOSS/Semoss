package prerna.sablecc2.reactor.insights.recipemanagement;

import java.util.Map;

import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetPipelineReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Map<String, Object> pipelineReturn = PixelUtility.generatePipeline(this.insight);
		return new NounMetadata(pipelineReturn, PixelDataType.MAP);
	}

}
