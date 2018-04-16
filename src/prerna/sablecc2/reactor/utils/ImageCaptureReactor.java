package prerna.sablecc2.reactor.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PushbackReader;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.InsightParamTranslation;
import prerna.sablecc2.PixelPreProcessor;
import prerna.sablecc2.lexer.Lexer;
import prerna.sablecc2.lexer.LexerException;
import prerna.sablecc2.node.Start;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.parser.Parser;
import prerna.sablecc2.parser.ParserException;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ImageCaptureReactor  extends AbstractReactor {

	private static final String CLASS_NAME = ImageCaptureReactor.class.getName();
	// get the directory separator
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();
	
	public ImageCaptureReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.URL.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String engineName = this.keyValue.get(this.keysToGet[0]);
		String feUrl = this.keyValue.get(this.keysToGet[1]);

		IEngine coreEngine = Utility.getEngine(engineName);
		// loop through the insights
		IEngine insightsEng = coreEngine.getInsightDatabase();
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(insightsEng, "select distinct id from question_id");
		while(wrapper.hasNext()) {
			String id = wrapper.next().getValues()[0] + "";
			logger.info("Start image capture for insight id = " + id);
			runImageCapture(feUrl, engineName, id);
			logger.info("Done saving image for insight id = " + id);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	public static void runImageCapture(String feUrl, String engineName, String id) {
		IEngine coreEngine = Utility.getEngine(engineName);
		runImageCapture(feUrl, coreEngine, engineName, id);
	}

	public static void runImageCapture(String feUrl, IEngine coreEngine, String engineName, String id) {
		Insight insight = null;
		try {
			insight = coreEngine.getInsight(id).get(0);
		} catch(Exception e) {
			e.printStackTrace();
		}
		if(insight instanceof OldInsight) {
			return;
		}
		List<String> recipe = insight.getPixelRecipe();
		if(!hasParam(recipe)) {
			String cmd = getCmd(feUrl, insight);
			Process p = null;
			try {
				p = Runtime.getRuntime().exec(cmd);
				while(p.isAlive()) {
					try {
						p.waitFor();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				// destroy it
				if(p != null) {
					p.destroy();
				} else {
					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
				}
			}
		}
	}
	
	/**
	 * See if the insight has a parameter
	 * @param recipe
	 * @return
	 */
	public static boolean hasParam(List<String> recipe) {
		InsightParamTranslation translation = new InsightParamTranslation();
		for(String expression : recipe) {
			expression = PixelPreProcessor.preProcessPixel(expression.trim(), new HashMap<String, String>());
			try {
				Parser p = new Parser(new Lexer(new PushbackReader(new InputStreamReader(new ByteArrayInputStream(expression.getBytes("UTF-8"))), expression.length())));
				// parsing the pixel - this process also determines if expression is syntactically correct
				Start tree = p.parse();
				// apply the translation.
				tree.apply(translation);
				
				// is it a param?
				if(translation.hasParam()) {
					return true;
				}
			} catch (ParserException | LexerException | IOException e) {
				e.printStackTrace();
			}
		}
		
		// isn't a param
		return false;
	}

	private static String getCmd(String feUrl, Insight in) {
		String id = in.getRdbmsId();
		String engine = in.getEngineName();

		String imageDirStr = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + 
				DIR_SEPARATOR + "db" + 
				DIR_SEPARATOR + engine + 
				DIR_SEPARATOR + "version" +
				DIR_SEPARATOR + id;
		
		File imageDir = new File(imageDirStr);
		if(!imageDir.exists()) {
			imageDir.mkdirs();
		}

		String googleHome = DIHelper.getInstance().getProperty(Constants.GOOGLE_CHROME_HOME);
		if(googleHome == null) {
			googleHome = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe";
		}
		
		if(googleHome.contains(" ")) {
			googleHome = "\"" + googleHome + "\"";
		}
		
		String insecure = "";
		if(feUrl.contains("localhost") && feUrl.contains("https")) {
			insecure = "--allow-insecure-localhost ";
		}
		
		String cmd = googleHome 
				+ " "
				+ "--headless "
				+ "--disable-gpu "
				+ "--window-size=1440,1440 "
				+ "--virtual-time-budget=10000 "
				+ insecure
				+ "--screenshot=\"" + imageDirStr + DIR_SEPARATOR + "image.png\" "
				+ "\"" + feUrl + "#!/insight?type=single&engine=" + engine + "&id=" + id + "&panel=0&drop=1000\"";

		return cmd;
	}
}
