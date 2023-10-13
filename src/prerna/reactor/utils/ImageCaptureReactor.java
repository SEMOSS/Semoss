package prerna.reactor.utils;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.om.ThreadStore;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.PixelUtility;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class ImageCaptureReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(ImageCaptureReactor.class);

	private static final String CLASS_NAME = ImageCaptureReactor.class.getName();
	// get the directory separator
	protected static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	public ImageCaptureReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.URL.getKey(), 
				ReactorKeysEnum.PARAM_KEY.getKey(), ReactorKeysEnum.IMAGE_WAIT_TIME.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		logger.info("Starting image capture...");
		logger.info("Operation can take up to 10 seconds to complete");
		
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		String feUrl = this.keyValue.get(this.keysToGet[1]);
		String param = this.keyValue.get(this.keysToGet[2]);
		String sessionId = ThreadStore.getSessionId();
		
		Integer waitTime = null;
		String waitTimeStr = this.keyValue.get(this.keysToGet[3]);
		if(waitTimeStr != null && (waitTimeStr=waitTimeStr.trim()).isEmpty()) {
			try {
				waitTime = Integer.parseInt(waitTimeStr);
			} catch(NumberFormatException e) {
				throw new IllegalArgumentException("Invalid wait time option = '" + waitTimeStr + "'. Error is: " + e.getMessage());
			}
		}
		
		IProject coreProject = Utility.getProject(projectId);
		// loop through the insights
		IDatabaseEngine insightsEng = coreProject.getInsightDatabase();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(insightsEng, "select distinct id from question_id");
			while(wrapper.hasNext()) {
				String id = wrapper.next().getValues()[0] + "";
				runImageCapture(feUrl, projectId, id, param, sessionId, waitTime);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper!=null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		ClusterUtil.pushProject(projectId);
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	public static void runImageCapture(String feUrl, String projectId, String insightId, String params, String sessionId, Integer waitTime) {
		IProject coreProject = Utility.getProject(projectId);
		if(coreProject == null) {
			throw new IllegalArgumentException("Cannot find project = " + projectId);
		}
		runImageCapture(feUrl, coreProject, projectId, insightId, params, sessionId, waitTime);
	}
	
	public static void runImageCapture(String feUrl, IProject coreProject, String appId, String insightId, String params, String sessionId, Integer waitTime) {
		Insight insight = null;
		try {
			insight = coreProject.getInsight(insightId).get(0);
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		if(insight instanceof OldInsight) {
			return;
		}
		List<String> recipe = insight.getPixelList().getPixelRecipe();
		if(params != null || !PixelUtility.isNotCacheable(recipe)) {
			runHeadlessChrome(feUrl, insight, params, sessionId, waitTime);
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
//						classLogger.error(Constants.STACKTRACE, e);
//					}
//				}
//			} catch (IOException e) {
//				classLogger.error(Constants.STACKTRACE, e);
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
	private static void runHeadlessChrome(String feUrl, Insight insight, String params, String sessionId, Integer waitTime) {
		feUrl = feUrl.trim();
		String id = insight.getRdbmsId();
		String projectId = insight.getProjectId();
		String projectName = insight.getProjectName();
//		String baseFolder = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String imageDirStr = "";
		if(params == null) {
//			imageDirStr = baseFolder + 
//				DIR_SEPARATOR + "db" + 
//				DIR_SEPARATOR + SmssUtilities.getUniqueName(engineName, engineId) + 
//				DIR_SEPARATOR + "version" +
//				DIR_SEPARATOR + id;
			imageDirStr = AssetUtility.getProjectVersionFolder(projectName, projectId) + DIR_SEPARATOR + id;
		} else {
			// params is already encodeed
			imageDirStr = AssetUtility.getProjectVersionFolder(projectName, projectId) +
					DIR_SEPARATOR + id + 
					DIR_SEPARATOR + "params" + 
					DIR_SEPARATOR + params;
		}
		
		String url = null;
		if(params != null) {
			url = feUrl+ "#!/insight?type=multi&engine=" + projectId + "&id=" + id + "&parameters=" + params +  "&hideMenu=true&drop=5000&animation=false";
		} else {
			url = feUrl+ "#!/insight?type=multi&engine=" + projectId + "&id=" + id + "&hideMenu=true&drop=5000&animation=false";
		}
		insight.getChromeDriver().captureImage(feUrl, url, imageDirStr + DIR_SEPARATOR + "image.png", sessionId, waitTime);
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
