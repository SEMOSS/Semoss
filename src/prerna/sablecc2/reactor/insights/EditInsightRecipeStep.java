package prerna.sablecc2.reactor.insights;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.om.Pixel;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class EditInsightRecipeStep extends AbstractReactor {

	private static final String CLASS_NAME = EditInsightRecipeStep.class.getName();
	
	public EditInsightRecipeStep() {
		this.keysToGet = new String[] {ReactorKeysEnum.PIXEL_ID.getKey(), ReactorKeysEnum.PIXEL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String pixelId = this.keyValue.get(this.keysToGet[0]);
		String encodedRecipe = this.keyValue.get(this.keysToGet[1]);
		if(pixelId == null || pixelId.isEmpty()) {
			throw new NullPointerException("Pixel id cannot be null");
		}
		if(encodedRecipe == null || encodedRecipe.isEmpty()) {
			throw new NullPointerException("Pixel string for the substitution cannot be null");
		}
		
		Logger logger = getLogger(CLASS_NAME);
		String recipe = Utility.decodeURIComponent(encodedRecipe);
		Pixel p = this.insight.getPixelList().getPixel(pixelId);
		p.modifyPixelString(recipe);
		
		// now i need to rerun the insight recipe
		// clear the insight
		// and re-run it
		logger.info("Re-executing the insight recipe... please wait as this operation may take some time");
		PixelRunner runner = this.insight.reRunPixelInsight();
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.EDIT_INSIGHT_RECIPE);
		return noun;
	}

}
