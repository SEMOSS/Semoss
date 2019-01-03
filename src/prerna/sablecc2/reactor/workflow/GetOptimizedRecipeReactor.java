package prerna.sablecc2.reactor.workflow;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.HashMap;
import java.util.List;

import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.translations.OptimizeRecipeTranslation;

public class GetOptimizedRecipeReactor extends AbstractReactor {
	
	/**
	 * This reactor creates an optimized recipe - it removes from the recipe instances of pixels with TaskOptions that are not needed
	 * No inputs are used
	 */
	
	@Override
	public NounMetadata execute() {
		// grab the full recipe
		List<String> pixelRecipe = this.insight.getPixelRecipe();
		// return the modified recipe
		return new NounMetadata(getModifiedRecipeString(pixelRecipe), PixelDataType.CONST_STRING, PixelOperationType.CURRENT_INSIGHT_RECIPE);
	}
	
	/**
	 * This method is used to retrieve the recipe as a String rather than as List<String>
	 * 
	 * @param recipe
	 * @return modifiedRecipe
	 */
	public String getModifiedRecipeString(List<String> recipe) {
		List<String> modifiedRecipeList = getOptimizedRecipe(recipe);
		return recipeToString(modifiedRecipeList);
	}
	
	/**
	 * This method converts the recipe from List<String> to a single string
	 * 
	 * @param recipe
	 * @return recipe (as String)
	 */
	private static String recipeToString(List<String> recipe) {
		StringBuilder sb = new StringBuilder();
		// iterate through the recipe and build as a string builder
		for (int i = 0; i < recipe.size(); i++) {
			sb.append(recipe.get(i));
		}
		return sb.toString();
	}
	
	/**
	 * This method is used to get the modified recipe based on the full recipe
	 * retrieves as List<String>
	 * 
	 * @param recipe
	 * @return modifiedRecipe
	 */
	public List<String> getOptimizedRecipe(List<String> recipe) {
		// create a new translation object that will do the work of figuring out which pixel expressions to keep
		OptimizeRecipeTranslation translation = new OptimizeRecipeTranslation();
		// we want to iterate through the current recipe
		for (int i = 0; i < recipe.size(); i++) {
			String expression = recipe.get(i);
			// fill in the encodedToOriginal with map for the current expression
			expression = PixelPreProcessor.preProcessPixel(expression.trim(), translation.encodedToOriginal);
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation
				// when we apply the translation, we will change encoded expressions back to their original form
				tree.apply(translation);
				// reset translation.encodedToOriginal for each expression
				translation.encodedToOriginal = new HashMap<String, String>();
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		// we want to run the finalizeExpressionsToKeep method only after all expressions have been run
		// this way we can find the last expression index used 
		List<String> modifiedRecipe = translation.finalizeExpressionsToKeep();
		return modifiedRecipe;
	}
}
