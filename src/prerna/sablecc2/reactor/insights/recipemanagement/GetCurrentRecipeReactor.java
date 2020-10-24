package prerna.sablecc2.reactor.insights.recipemanagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetCurrentRecipeReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		PixelList pixelList = this.insight.getPixelList();
		
		List<Map<String, String>> retList = new ArrayList<>();
		// Don't add GetCurrentRecipe to the end
		for (int i = 0; i < pixelList.size() - 1; i++) {
			Pixel p = pixelList.get(i);
			Map<String, String> innerMap = new HashMap<>();
			innerMap.put("id", p.getId());
			innerMap.put("expression", p.getPixelString());
			retList.add(innerMap);
		}
		return new NounMetadata(retList, PixelDataType.MAP, PixelOperationType.CURRENT_INSIGHT_RECIPE);
	}

}
