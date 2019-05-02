package prerna.sablecc2.reactor.workflow;

import java.util.List;
import java.util.Map;

import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetInsightDatasourcesReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> recipe = this.insight.getPixelRecipe();
		StringBuilder b = new StringBuilder();
		for(String s : recipe) {
			b.append(s);
		}
		String fullRecipe = b.toString();
		
		List<Map<String, Object>> sourcePixels = PixelUtility.getDatasourcesMetadata(this.insight.getUser(), fullRecipe);
		return new NounMetadata(sourcePixels, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

}
