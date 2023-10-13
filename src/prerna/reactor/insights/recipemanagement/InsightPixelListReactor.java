package prerna.reactor.insights.recipemanagement;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class InsightPixelListReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		return new NounMetadata(this.insight.getPixelList(), PixelDataType.VECTOR);
	}

}
