package prerna.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.io.StringBufferInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.DbTranslationEditor;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;

public class AppNameRecipeModifier {

	private static final String GET_ALL_RECIPES = "SELECT DISTINCT ID, QUESTION_PKQL FROM QUESTION_ID";

	private AppNameRecipeModifier() {

	}

	/**
	 * Change all the locations of a database name with a new name
	 * @param smssFile
	 * @param rdbmsInsightsLocation
	 * @param newNameForDb
	 * @throws UnsupportedEncodingException 
	 */
	public static void renameDatabaseForInsights(String rdbmsInsightsLocation, String newNameForDb, String origEngineName) throws UnsupportedEncodingException {
		// in case we need to prefill the location
		Map<String, String> paramHash = new Hashtable<String, String>();
		paramHash.put("engine", newNameForDb);
		rdbmsInsightsLocation = Utility.fillParam2(rdbmsInsightsLocation, paramHash);

		String jdbcURL = "jdbc:h2:" + rdbmsInsightsLocation + ";query_timeout=180000;early_filter=true;query_cache_size=24;cache_size=32768";
		String userName = "sa";
		String password = "";

		RDBMSNativeEngine rne = new RDBMSNativeEngine();
		rne.makeConnection("org.h2.Driver", jdbcURL, userName, password);

		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(rne, GET_ALL_RECIPES);
		while(wrapper.hasNext()) {
			IHeadersDataRow dataRow = wrapper.next();
			Object[] values = dataRow.getValues();
			String rdbmsId = values[0].toString();
			Object[] origPixel = (Object[]) values[1];

			// need to make sure we dont destroy things that are legacy
			if(origPixel != null) {
				int numSteps = origPixel.length;
				StringBuilder collapsedPixelRecipe = new StringBuilder();
				for(int i = 0; i < numSteps; i++) {
					collapsedPixelRecipe.append(origPixel[i]);
				}
				List<String> newPixels = modifyPixelDatabaseName(collapsedPixelRecipe.toString(), origEngineName, newNameForDb);
				// newPixels is null if there were no modifications required for this insight
				if(newPixels != null) {
					// okay, we need to generate an update query
					//TODO: make this a prepared statement

					StringBuilder updateQuery = new StringBuilder("UPDATE QUESTION_ID SET QUESTION_PKQL =");
					updateQuery.append(InsightAdministrator.getArraySqlSyntax(newPixels));
					updateQuery.append(" WHERE ID='").append(rdbmsId).append("'");

					try {
						rne.insertData(updateQuery.toString());
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
			}
		}
		
		rne.closeDB();
	}

	private static List<String> modifyPixelDatabaseName(String pixel, String engineToFind, String engineToReplace) throws UnsupportedEncodingException {
		Map<String, String> encodedTextToOriginal = new HashMap<String, String>();
		pixel = PixelPreProcessor.preProcessPixel(pixel, encodedTextToOriginal);
		
		Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(pixel.getBytes("UTF-8")), "UTF-8"), pixel.length())));
		DbTranslationEditor translation = new DbTranslationEditor();
		translation.setEngineToFind(engineToFind);
		translation.setEngineToReplace(engineToReplace);

		try {
			// parsing the pkql - this process also determines if expression is syntactically correct
			Start tree = p.parse();
			// apply the translation.
			tree.apply(translation);
		} catch (ParserException | LexerException | IOException e) {
			e.printStackTrace();
		}

		if(translation.isNeededModifcation()) {
			return translation.getPixels();
		} else {
			return null;
		}
	}

}
