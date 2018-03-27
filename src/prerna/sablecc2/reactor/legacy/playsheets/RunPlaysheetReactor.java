package prerna.sablecc2.reactor.legacy.playsheets;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;

import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.ui.helpers.OldInsightProcessor;
import prerna.util.Utility;

public class RunPlaysheetReactor extends AbstractReactor {
	public RunPlaysheetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey(),
				ReactorKeysEnum.PARAM_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String app = this.keyValue.get(this.keysToGet[0]);
		String insightId = this.keyValue.get(this.keysToGet[1]);
		IEngine engine = Utility.getEngine(app);
		Insight insightObj = InsightStore.getInstance().findInsightInStore(app, insightId);
		Object obj = null;
		// Get the Insight, grab its ID
		insightObj = engine.getInsight(insightId).get(0);
		// set the user id into the insight
		insightObj.setUser(this.insight.getUser());
		Map<String, List<Object>> params = getParamMap();
		if (insightObj.isOldInsight()) {
			((OldInsight) insightObj).setParamHash(params);
		}
		// check if the insight has already been cached
		try {
			InsightStore.getInstance().put(insightObj);
			if (insightObj.isOldInsight()) {
				// we have some old legacy stuff...
				// just run and return the object
				OldInsightProcessor processor = new OldInsightProcessor((OldInsight) insightObj);
				obj = processor.runWeb();
				((Map) obj).put("isPkqlRunnable", false);
				((Map) obj).put("recipe", new Object[0]);

				// TODO: why did we allow the FE to still require this when
				// we already pass a boolean that says this is not pkql....
				// wtf...

				HashMap insightMap = new HashMap();
				Map stuipdFEInsightGarabage = new HashMap();
				stuipdFEInsightGarabage.put("clear", false);
				stuipdFEInsightGarabage.put("closedPanels", new Object[0]);
				stuipdFEInsightGarabage.put("dataID", 0);
				stuipdFEInsightGarabage.put("feData", new HashMap());
				stuipdFEInsightGarabage.put("insightID", insightObj.getInsightId());
				stuipdFEInsightGarabage.put("newColumns", new HashMap());
				stuipdFEInsightGarabage.put("newInsights", new Object[0]);
				stuipdFEInsightGarabage.put("pkqlData", new Object[0]);
				insightMap.put("insights", new Object[] { stuipdFEInsightGarabage });
				((Map) obj).put("pkqlOutput", insightMap);
			} else {
				// TODO: this should no longer be used
				// TODO: this should no longer be used
				// TODO: this should no longer be used
				// TODO: this should no longer be used
				// it is fully encapsulated in pixel

				obj = new HashMap<String, String>();
				((Map) obj).put("recipe", insightObj.getPixelRecipe());
				((Map) obj).put("rdbmsID", insightObj.getRdbmsId());
				((Map) obj).put("insightID", insightObj.getInsightId());
				((Map) obj).put("title", insightObj.getInsightName());

				// this is only necessary to get dashboards to work...
				// String layout = insightObj.getOutput();
				// ((Map) obj).put("layout", layout);
				// if(layout.equalsIgnoreCase("dashboard")) {
				// ((Map) obj).put("dataMakerName", "Dashboard");
				// }
			}
		} catch (Exception ex) { // need to specify the different exceptions
			ex.printStackTrace();
			SemossPixelException exception = new SemossPixelException();
			exception.setContinueThreadOfExecution(false);
			exception.setAdditionalReturn(new NounMetadata("Error occured processing question.", PixelDataType.ERROR, PixelOperationType.OLD_INSIGHT));
			throw exception;
		}
		// update security db user tracker
		// tracker.trackInsightExecution(userId,engine.app,insightObj.insightId,session.getId());
		// update global solr tracker
		try {
			SolrIndexEngine.getInstance().updateViewedInsight(engine.getEngineName() + "_" + insightId);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
				| IOException e) {
			e.printStackTrace();
		}
		return new NounMetadata(obj, PixelDataType.MAP, PixelOperationType.OLD_INSIGHT);
	}

	/**
	 * Get the params for the method
	 * 
	 * @return
	 */
	private Map<String, List<Object>> getParamMap() {
		GenRowStruct mapGrs = this.store.getNoun(this.keysToGet[2]);
		if (mapGrs != null && !mapGrs.isEmpty()) {
			return (Map<String, List<Object>>) mapGrs.get(0);
		}

		if (!curRow.isEmpty()) {
			return (Map<String, List<Object>>) curRow.get(1);
		}

		return new Hashtable<String, List<Object>>();
	}

}
