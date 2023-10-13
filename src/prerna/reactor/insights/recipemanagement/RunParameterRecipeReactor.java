package prerna.reactor.insights.recipemanagement;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.om.Pixel;
import prerna.query.parsers.ParamStruct;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelRunner;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.ParserException;
import prerna.util.Constants;
import prerna.util.Utility;

public class RunParameterRecipeReactor extends AbstractReactor {

	private static final String CLASS_NAME = RunParameterRecipeReactor.class.getName();
	
	public static final String FILL_RECIPE_KEY = "fill";
	
	public RunParameterRecipeReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.RECIPE.getKey(), FILL_RECIPE_KEY};
	}
	
	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		List<String> recipeInput = getRecipe();
		List<String> recipeToRun = null;
		if(recipeInput.isEmpty()) {
			throw new NullPointerException("Must pass in a new recipe");
		}
		
		if(fillRecipe()) {
			recipeToRun = new Vector<>(recipeInput.size());
			// we fill in based on existing parameter values
			Map<String, NounMetadata> paramInputs = this.insight.getVarStore().pullParameters();
			for(String paramPixel : recipeInput) {
				String filledPixel = paramPixel;
				for(String paramKey : paramInputs.keySet()) {
					NounMetadata paramNoun = paramInputs.get(paramKey);
					ParamStruct pStruct = (ParamStruct) paramNoun.getValue();
					String paramLookup = "<" + pStruct.getParamName() + ">";
					String paramReplacement = pStruct.getDetailsList().get(0).getPixelStringReplacement(pStruct.getDefaultValue());
					// for some reason
					// on FE, we do "<paramName>" for all model views except for checklist we use [<paramName>]
					// so dont want to have the duplicate quotes
					if(!ParamStruct.PARAM_FILL_USE_ARRAY_TYPES.contains(pStruct.getModelDisplay())) {
						if(paramReplacement.startsWith("\"") && paramReplacement.endsWith("\"")) {
							paramReplacement = paramReplacement.substring(1, paramReplacement.length()-1);
						} else if(paramReplacement.startsWith("'") && paramReplacement.endsWith("'")) {
							paramReplacement = paramReplacement.substring(1, paramReplacement.length()-1);
						}
					}
					filledPixel = filledPixel.replace(paramLookup, paramReplacement);
				}
				// add back
				recipeToRun.add(filledPixel);
			}
		} else {
			recipeToRun = recipeInput;
		}
		
		// now i need to rerun the insight recipe
		// clear the insight
		// and re-run it
		logger.info("Re-executing the insight recipe... please wait as this operation may take some time");
		List<String> pixelList = new Vector<String>();

		try {
			for(String pixelString : recipeToRun) {
				List<String> breakdown = PixelUtility.parsePixel(pixelString);
				pixelList.addAll(breakdown);
			}
		} catch (ParserException | LexerException | IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred parsing the pixel expression");
		}
		// set the new pixel recipe
		this.insight.setPixelRecipe(pixelList);
		// rerun the recipe
		PixelRunner runner = this.insight.reRunPixelInsight(false, true);
		
		// THIS IS BECAUSE SOMEONE MADE REALLY ANNOYING TAP CORE PARAMETER INSIGHTS
		// AND DIDN'T PUT AddPanel(0) INSIDE THE QUERY PORTION OF THE JSON
		{
			if(this.insight.getInsightPanels().containsKey("0")) {
				List<Pixel> runnerPixelReturn = runner.getReturnPixelList();
				Pixel meta = new Pixel("meta", "AddPanel(\"0\")");
				meta.setMeta(true);
				runnerPixelReturn.add(0, meta);
				List<NounMetadata> runnerResults = runner.getResults();
				runnerResults.add(0, new NounMetadata(insight.getInsightPanel("0"), 
						PixelDataType.PANEL, PixelOperationType.PANEL_OPEN));
			}
		}
		
		// return the recipe steps
		Map<String, Object> runnerWraper = new HashMap<String, Object>();
		runnerWraper.put("runner", runner);
		NounMetadata noun = new NounMetadata(runnerWraper, PixelDataType.PIXEL_RUNNER, PixelOperationType.PARAMETER_EXECUTION);
		return noun;
	}
	
	/**
	 * Get the input recipe
	 * @return
	 */
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
			List<String> values = this.curRow.getAllStrValues();
			if(values != null && !values.isEmpty()) {
				for(String pixel : values) {
					recipe.add(Utility.decodeURIComponent(pixel));
				}
			}
		}
		
		return recipe;
	}
	
	/**
	 * Determine if BE is filling the recipe
	 * @return
	 */
	private Boolean fillRecipe() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return Boolean.parseBoolean(grs.get(0) + "");
		}
		
		return false;
	}
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals(FILL_RECIPE_KEY)) {
			return "Boolean to determine if the BE should be filling in the recipe. Requires the recipe to be parameterized";
		}
		return super.getDescriptionForKey(key);
	}

}
