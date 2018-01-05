package prerna.sablecc2.reactor.insights;

import java.util.List;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetCurrentRecipeReactor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> pixelRecipe = this.insight.getPixelRecipe();
		StringBuilder recipe = new StringBuilder();
		for (String pixel : pixelRecipe) {
			recipe.append(pixel);
		}
		return new NounMetadata(recipe.toString(), PixelDataType.CONST_STRING, PixelOperationType.CURRENT_INSIGHT_RECIPE);
	}

}
