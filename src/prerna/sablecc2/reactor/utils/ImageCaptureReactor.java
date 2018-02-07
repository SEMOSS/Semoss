package prerna.sablecc2.reactor.utils;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.om.OldInsight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ImageCaptureReactor  extends AbstractReactor {

	private static final String CLASS_NAME = ExportDatabaseReactor.class.getName();
	
	public ImageCaptureReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
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
			Insight insight = coreEngine.getInsight(id).get(0);
			if(insight instanceof OldInsight) {
				continue;
			}
			String cmd = getCmd(insight);
			logger.info("Running : " + cmd);
			
			try {
				Process p = Runtime.getRuntime().exec(cmd);
				while(p.isAlive()) {
					try {
						p.waitFor();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				
				Map<String, Object> solrInsights = new HashMap<>();
				solrInsights.put(SolrIndexEngine.IMAGE, "\\db\\" + engineName + "\\version\\" + id + "\\image.png");
				try {
					SolrIndexEngine.getInstance().modifyInsight(engineName + "_" + id, solrInsights);
				} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
	
	private static String getCmd(Insight in) {
		String id = in.getRdbmsId();
		String engine = in.getEngineName();
		
		String imageDirStr = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\db\\" + engine + "\\version\\" + id;
		File imageDir = new File(imageDirStr);
		if(!imageDir.exists()) {
			imageDir.mkdirs();
		}
		
		String cmd = "\"C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe\" "
				+ "--headless "
				+ "--disable-gpu "
				+ "--window-size=2560,1440 "
				+ "--virtual-time-budget=10000 "
				+ "--screenshot=\"" + imageDirStr + "\\image.png\" "
				+ "\"http://localhost:8080/SemossWeb_App/#!/insight?type=single&engine=" + engine + "&id=" + id + "&panel=0\"";
		
		return cmd;
	}
}
