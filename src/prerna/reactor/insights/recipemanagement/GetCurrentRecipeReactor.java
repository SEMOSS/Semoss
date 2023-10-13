package prerna.reactor.insights.recipemanagement;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetCurrentRecipeReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		PixelList pixelList = this.insight.getPixelList();
		
		List<Map<String, Object>> retList = new ArrayList<>();
		// Don't add GetCurrentRecipe to the end
		for (int i = 0; i < pixelList.size(); i++) {
			Pixel p = pixelList.get(i);
			Map<String, Object> innerMap = new HashMap<>();
			innerMap.put("id", p.getId());
			innerMap.put("expression", p.getPixelString());
			innerMap.put("error", p.isReturnedError());
			innerMap.put("errorMessages", p.getErrorMessages());
			innerMap.put("warning", p.isReturnedWarning());
			innerMap.put("warningMessages", p.getWarningMessages());
			retList.add(innerMap);
		}
		return new NounMetadata(retList, PixelDataType.MAP, PixelOperationType.CURRENT_INSIGHT_RECIPE);
	}

}
