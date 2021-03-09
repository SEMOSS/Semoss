package prerna.sablecc2.reactor.insights.recipemanagement;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class InsightPixelListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return new NounMetadata(this.insight.getPixelList(), PixelDataType.VECTOR);
	}

}
