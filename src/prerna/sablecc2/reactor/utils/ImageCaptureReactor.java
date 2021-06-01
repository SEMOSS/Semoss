package prerna.sablecc2.reactor.utils;

import java.util.List;

import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.om.ThreadStore;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.AssetUtility;
import prerna.util.ChromeDriverUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ImageCaptureReactor extends AbstractReactor {

	private static final String CLASS_NAME = ImageCaptureReactor.class.getName();
	// get the directory separator
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public ImageCaptureReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.URL.getKey(), ReactorKeysEnum.PARAM_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Starting image capture...");
		logger.info("Operation can take up to 10 seconds to complete");
		
		organizeKeys();
		String appId = this.keyValue.get(this.keysToGet[0]);
		String feUrl = this.keyValue.get(this.keysToGet[1]);
		String param = this.keyValue.get(this.keysToGet[2]);
		String sessionId = ThreadStore.getSessionId();
		
		IEngine coreEngine = Utility.getEngine(appId);
		// loop through the insights
		IEngine insightsEng = coreEngine.getInsightDatabase();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(insightsEng, "select distinct id from question_id");
			while(wrapper.hasNext()) {
				String id = wrapper.next().getValues()[0] + "";
				runImageCapture(feUrl, appId, id, param, sessionId);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}

		ClusterUtil.reactorPushApp(appId);
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	public static void runImageCapture(String feUrl, String appId, String insightId, String params, String sessionId) {
		IEngine coreEngine = Utility.getEngine(appId);
		if(coreEngine == null) {
			// we may have the alias
			List<String> appIds = MasterDatabaseUtility.getEngineIdsForAlias(appId);
			if(appIds.size() == 1) {
				coreEngine = Utility.getEngine(appIds.get(0));
			} else if(appIds.size() > 1) {
				throw new IllegalArgumentException("There are 2 databases with the name " + appId + ". Please pass in the correct id to know which source you want to load from");
			}
			
			if(coreEngine == null) {
				throw new IllegalArgumentException("Cannot find app = " + appId);
			}
		}
		runImageCapture(feUrl, coreEngine, appId, insightId, params, sessionId);
	}
	
	public static void runImageCapture(String feUrl, IEngine coreEngine, String appId, String insightId, String params, String sessionId) {
		Insight insight = null;
		try {
			insight = coreEngine.getInsight(insightId).get(0);
		} catch(Exception e) {
			e.printStackTrace();
		}
		if(insight instanceof OldInsight) {
			return;
		}
		List<String> recipe = insight.getPixelList().getPixelRecipe();
		if(params != null || !PixelUtility.isNotCacheable(recipe)) {
			runHeadlessChrome(feUrl, insight, params, sessionId);
		}
		
//		if(params != null || !PixelUtility.hasParam(recipe)) {
//			String[] cmd = getCmdArray(feUrl, insight, params);
//			Process p = null;
//			try {
//				p = new ProcessBuilder(cmd).start();
//				while(p.isAlive()) {
//					try {
//						p.waitFor();
//					} catch (InterruptedException e) {
//						e.printStackTrace();
//					}
//				}
//			} catch (IOException e) {
//				e.printStackTrace();
//			} finally {
//				// destroy it
//				if(p != null) {
//					p.destroy();
//				} else {
//					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
//					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
//					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
//					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
//					System.out.println("ERROR RUNNING IMAGE CAPTURE!!!");
//				}
//			}
//		}
	}

	/**
	 * Run headless chrome via selenium
	 * @param feUrl
	 * @param insight
	 * @param params
	 * @param sessionId
	 */
	private static void runHeadlessChrome(String feUrl, Insight insight, String params, String sessionId) {
		feUrl = feUrl.trim();
		String id = insight.getRdbmsId();
		String engineId = insight.getEngineId();
		String engineName = insight.getEngineName();
		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String imageDirStr = "";
		if(params == null) {
//			imageDirStr = baseFolder + 
//				DIR_SEPARATOR + "db" + 
//				DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) + 
//				DIR_SEPARATOR + "version" +
//				DIR_SEPARATOR + id;
			imageDirStr = AssetUtility.getAppAssetVersionFolder(engineName, engineId) + DIR_SEPARATOR + id;
		} else {
			// params is already encodeed
			imageDirStr = AssetUtility.getAppAssetVersionFolder(engineName, engineId) +
					DIR_SEPARATOR + id + 
					DIR_SEPARATOR + "params" + 
					DIR_SEPARATOR + params;
		}
		
		String url = null;
		if(params != null) {
			url = feUrl+ "#!/insight?type=multi&engine=" + engineId + "&id=" + id + "&parameters=" + params +  "&hideMenu=true&drop=5000&animation=false";
		} else {
			url = feUrl+ "#!/insight?type=multi&engine=" + engineId + "&id=" + id + "&hideMenu=true&drop=5000&animation=false";
		}
		insight.getChromeDriver().captureImage(feUrl, url, imageDirStr + DIR_SEPARATOR + "image.png", sessionId);
	}
	

	
//	private static String[] getCmdArray(String feUrl, Insight in, String params) {
//		String id = in.getRdbmsId();
//		String engineId = in.getEngineId();
//		String engineName = in.getEngineName();
//		
//		String imageDirStr = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + 
//				DIR_SEPARATOR + "db" + 
//				DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) + 
//				DIR_SEPARATOR + "version" +
//				DIR_SEPARATOR + id;
//		
//		if(params != null) {
//			imageDirStr += params;
//		}
//		
//		File imageDir = new File(imageDirStr);
//		if(!imageDir.exists()) {
//			imageDir.mkdirs();
//		}
//
//		String googleHome = DIHelper.getInstance().getProperty(Constants.GOOGLE_CHROME_HOME);
//		if(googleHome == null) {
//			googleHome = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe";
//		}
//		
//		String insecure = "";
//		if(feUrl.contains("localhost") && feUrl.contains("https")) {
//			insecure = "--allow-insecure-localhost ";
//		}
//		String[] cmd = null;
//		if(insecure.equals("")){
//			if(params != null) {
//				cmd = new String[]{googleHome,"--headless","--disable-gpu","--window-size=1440,1440","--virtual-time-budget=10000",
//						"--screenshot="+imageDirStr + DIR_SEPARATOR + "image.png",feUrl+ "#!/insight?type=single&engine=" + engineId + "&id=" + id + "&parameters=" + params +  "&hideMenu=true&panel=0&drop=1000"};	
//
//			} else {
//				cmd = new String[]{googleHome,"--headless","--disable-gpu","--window-size=1440,1440","--virtual-time-budget=10000",
//						"--screenshot="+imageDirStr + DIR_SEPARATOR + "image.png",feUrl+ "#!/insight?type=single&engine=" + engineId + "&id=" + id + "&hideMenu=true&panel=0&drop=1000"};	
//			}
//		}
//		else {
//			if(params != null) {
//				cmd = new String[]{googleHome,"--headless","--disable-gpu","--window-size=1440,1440","--virtual-time-budget=10000",insecure,
//						"--screenshot="+imageDirStr + DIR_SEPARATOR + "image.png",feUrl+ "#!/insight?type=single&engine=" + engineId + "&id=" + id + "&parameters=" + params + "&hideMenu=true&panel=0&drop=1000"};				
//			} else {
//				cmd = new String[]{googleHome,"--headless","--disable-gpu","--window-size=1440,1440","--virtual-time-budget=10000",insecure,
//						"--screenshot="+imageDirStr + DIR_SEPARATOR + "image.png",feUrl+ "#!/insight?type=single&engine=" + engineId + "&id=" + id + "&hideMenu=true&panel=0&drop=1000"};	
//			}
//		}
//		return cmd;
//	}

}
