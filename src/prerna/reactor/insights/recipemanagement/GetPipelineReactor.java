package prerna.reactor.insights.recipemanagement;

import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetPipelineReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		Map<String, Object> pipelineReturn = PixelUtility.generatePipeline(this.insight);
		return new NounMetadata(pipelineReturn, PixelDataType.MAP);
	}

}
