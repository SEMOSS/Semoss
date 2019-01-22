package prerna.sablecc2;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class DbTranslationEditor extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(DbTranslationEditor.class.getName());

	// this will store the list of pixels that were passed in
	private List<String> pixels = new Vector<String>();
	// a replacement for the given input
	private String origPixelPortion = null;
	private String replacementPixel = null;

	// set the engine name to find and replace
	private String engineToReplace = null;
	private String engineToFind = null;

	private boolean neededModifcation = false;

	@Override
	public void caseAConfiguration(AConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
		for(PRoutine e : copy) {
			String expression = e.toString();
			LOGGER.info("Processing " + expression);
			e.apply(this);
			// if we ended up making the modificaiton
			// the replacement string will not be null
			if(this.replacementPixel != null && this.origPixelPortion != null) {
				String newExpression = expression.replace(this.origPixelPortion, this.replacementPixel);
				this.pixels.add(newExpression);
				// now we need to null replacement
				this.replacementPixel = null;
				this.origPixelPortion = null;

				// set that we needed some kind of modication
				this.neededModifcation = true;
			} else {
				this.pixels.add(expression);
			}
		}
	}


	@Override
	public void inAOperation(AOperation node) {
		defaultIn(node);

		String reactorId = node.getId().toString().trim();
		if(reactorId.equals("Database")) {
			// right now, the only input for database is in the curRow
			String dbInput = node.getOpInput().toString().trim();
			if(dbInput.equals(this.engineToFind)) {
				// okay, it is a match
				// we need to replace
				this.origPixelPortion = node.toString();
				this.replacementPixel = node.toString().replace(this.engineToFind, this.engineToReplace);
			}
		}
	}

	public void setEngineToReplace(String engineToReplace) {
		this.engineToReplace = engineToReplace;
	}

	public void setEngineToFind(String engineToFind) {
		this.engineToFind = engineToFind;
	}

	public List<String> getPixels() {
		return pixels;
	}

	public boolean isNeededModifcation() {
		return neededModifcation;
	}


	public static void main(String[] args) throws Exception {
		//		String expression = "CreateFrame(py); Database(Movie_RDBMS) | Select(Title, Title__Movie_Budget) | Import();";
		//		DbTranslationEditor translation = new DbTranslationEditor();
		//		translation.setEngineToFind("Movie_RDBMS");
		//		translation.setEngineToReplace("MyMovie");
		//
		//		try {
		//			Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
		//			// parsing the pkql - this process also determines if expression is syntactically correct
		//			Start tree = p.parse();
		//			// apply the translation.
		//			tree.apply(translation);
		//		} catch (ParserException | LexerException | IOException e) {
		//			e.printStackTrace();
		//		}
		//		
		//		System.out.println(translation.pixels);


		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId(Constants.LOCAL_MASTER_DB_NAME);
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty(Constants.LOCAL_MASTER_DB_NAME, coreEngine);

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\Movie.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("Movie");
		coreEngine.openDB(engineProp);
		DIHelper.getInstance().setLocalProperty("Movie", coreEngine);

		IEngine insightEngine = coreEngine.getInsightDatabase();
		InsightAdministrator admin = new InsightAdministrator(insightEngine);

		// query the insights rdbms and get all the insights
		// also grab the layout... annoying
		String query = "select id, question_layout from question_id";
		IRawSelectWrapper idWrapper = WrapperManager.getInstance().getRawWrapper(insightEngine, query);
		while(idWrapper.hasNext()) {
			Object[] row = idWrapper.next().getValues();
			String insightId = row[0].toString();
			String layout = row[1].toString();
			Insight in = coreEngine.getInsight(insightId).get(0);
			if(in instanceof OldInsight) {
				// ignore
				continue;
			}

			// get the old
			List<String> oldRecipe = in.getPixelRecipe();
			// store the new
			List<String> newRecipe = new Vector<String>();
			
			for(String expression : oldRecipe) {
				DbTranslationEditor translation = new DbTranslationEditor();
				translation.setEngineToFind("MovieDatabase");
				translation.setEngineToReplace("Movie");
				
				try {
					expression = PixelPreProcessor.preProcessPixel(expression.trim(), new HashMap<String, String>());
					Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
					// parsing the pkql - this process also determines if expression is syntactically correct
					Start tree = p.parse();
					// apply the translation.
					tree.apply(translation);

					// get the new recipe
					newRecipe.addAll(translation.pixels);
				} catch (ParserException | LexerException | IOException e) {
					e.printStackTrace();
				}
			}
			
			// now i am done looping through
			// so update the recipe for the insight
			LOGGER.info("UPDATING INSIGHT ID = " + in.getRdbmsId());
			admin.updateInsight(in.getRdbmsId(), in.getInsightName(), layout, newRecipe.toArray(new String[]{}));
		}

		LOGGER.info("DONE UPDATING ALL INSIGHT IDS");
	}

}
