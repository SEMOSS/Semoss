package prerna.sablecc2.reactor.insights;

import java.util.List;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetCurrentRecipeReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> pixelRecipe = this.insight.getPixelList().getPixelRecipe();
		StringBuilder recipe = new StringBuilder();
		
		// Don't add GetCurrentRecipe to the end
		for (int i = 0; i < pixelRecipe.size() - 1; i++) {
			recipe.append(pixelRecipe.get(i));
		}
		return new NounMetadata(recipe.toString(), PixelDataType.CONST_STRING, PixelOperationType.CURRENT_INSIGHT_RECIPE);
	}

}
