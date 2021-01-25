package prerna.sablecc2.reactor.insights.recipemanagement;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunParameterRecipeReactor extends AbstractReactor {

	private static final String CLASS_NAME = RunParameterRecipeReactor.class.getName();
	
	public RunParameterRecipeReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.RECIPE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		List<String> recipeInput = getRecipe();
		if(recipeInput.isEmpty()) {
			throw new NullPointerException("Must pass in a new recipe");
		}
		// now i need to rerun the insight recipe
		// clear the insight
		// and re-run it
		logger.info("Re-executing the insight recipe... please wait as this operation may take some time");
		List<String> pixelList = new Vector<String>();

		// temporary? as long as user isn't doing some crazy thing
		// this shouldn't affect anything
		{
			pixelList.add("AddPanel(0);");
		}
		try {
			for(String pixelString : recipeInput) {
				List<String> breakdown = PixelUtility.parsePixel(pixelString);
				pixelList.addAll(breakdown);
			}
		} catch (ParserException | LexerException | IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occured parsing the pixel expression");
		}
		// set the new pixel recipe
		this.insight.setPixelRecipe(pixelList);
		// rerun the recipe
		PixelRunner runner = this.insight.reRunPixelInsight(false);
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.RERUN_INSIGHT_RECIPE);
		return noun;
	}
	
	private List<String> getRecipe() {
		List<String> recipe = new Vector<>();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for(int i = 0; i < size; i++) {
				String pixel = grs.get(i) + "";
				recipe.add(Utility.decodeURIComponent(pixel));
			}
		}
		
		if(recipe.isEmpty()) {
			int size = this.curRow.size();
			for(int i = 0; i < size; i++) {
				String pixel = this.curRow.get(i) + "";
				recipe.add(Utility.decodeURIComponent(pixel));
			}
		}
		
		return recipe;
	}

}
