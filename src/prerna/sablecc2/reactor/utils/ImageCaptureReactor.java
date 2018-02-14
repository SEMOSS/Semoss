package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ImageCaptureReactor  extends AbstractReactor {

	private static final String CLASS_NAME = ImageCaptureReactor.class.getName();

	public ImageCaptureReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey() };
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String engineName = this.keyValue.get(this.keysToGet[0]);

		IEngine coreEngine = Utility.getEngine(engineName);
		// loop through the insights
		IEngine insightsEng = coreEngine.getInsightDatabase();
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(insightsEng, "select distinct id from question_id");
		while(wrapper.hasNext()) {
			String id = wrapper.next().getValues()[0] + "";
			logger.info("Start image capture for insight id = " + id);
			runImageCapture(engineName, id);
			logger.info("Done saving image for insight id = " + id);
		}

		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}

	public static void runImageCapture(String engineName, String id) {
		IEngine coreEngine = Utility.getEngine(engineName);
		runImageCapture(coreEngine, engineName, id);
	}

	public static void runImageCapture(IEngine coreEngine, String engineName, String id) {
		Insight insight = null;
		try {
			insight = coreEngine.getInsight(id).get(0);
		} catch(Exception e) {
			e.printStackTrace();
		}
		if(insight instanceof OldInsight) {
			return;
		}
		String cmd = getCmd(insight);
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
			p.destroy();
		}
	}

	private static String getCmd(Insight in) {
		String id = in.getRdbmsId();
		String engine = in.getEngineName();

		String imageDirStr = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\db\\" + engine + "\\version\\" + id;
		File imageDir = new File(imageDirStr);
		if(!imageDir.exists()) {
			imageDir.mkdirs();
		}

		String cmd = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe "
				+ "--headless "
				+ "--disable-gpu "
				+ "--window-size=1440,1440 "
				+ "--virtual-time-budget=10000 "
				+ "--screenshot=\"" + imageDirStr + "\\image.png\" "
				+ "\"http://localhost:8080/SemossWeb_AppUi/#!/insight?type=single&engine=" + engine + "&id=" + id + "&panel=0\"";

		return cmd;
	}
}
