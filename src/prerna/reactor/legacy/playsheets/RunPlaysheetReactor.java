package prerna.reactor.legacy.playsheets;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.om.OldInsight;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.reactor.insights.GlobalInsightCountUpdater;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.ui.helpers.OldInsightProcessor;
import prerna.util.Utility;
import prerna.util.insight.InsightUtility;

public class RunPlaysheetReactor extends AbstractReactor {
	
	public RunPlaysheetReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.PARAM_KEY.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = this.keyValue.get(this.keysToGet[0]);
		// TODO: ACCOUNTING FOR LEGACY PLAYSHEETS
		if(projectId == null) {
			projectId = this.store.getNoun("app").get(0) + "";
		}
		String insightId = this.keyValue.get(this.keysToGet[1]);
		IProject project = Utility.getProject(projectId);
		Insight insightObj = project.getInsight(insightId).get(0);
		InsightUtility.transferDefaultVars(this.insight, insightObj);

		// Get the Insight, grab its ID
		// set the user id into the insight
		insightObj.setUser(this.insight.getUser());
		Map<String, List<Object>> params = getParamMap();
		if(!insightObj.isOldInsight()) {
			throw new IllegalArgumentException("This is a legacy pixel that should only be used for old insights");
		}
		((OldInsight) insightObj).setParamHash(params);
		// store in insight store
		InsightStore.getInstance().put(insightObj);
		InsightStore.getInstance().addToSessionHash(getSessionId(), insightObj.getInsightId());

		// TODO: why did we allow the FE to still require this when
		// we already pass a boolean that says this is not pkql....
		// wtf...

		Map<String, Object> insightMap = new HashMap<String, Object>();
		Map<String, Object> stuipdFEInsightGarabage = new HashMap<String, Object>();
		stuipdFEInsightGarabage.put("clear", false);
		stuipdFEInsightGarabage.put("closedPanels", new Object[0]);
		stuipdFEInsightGarabage.put("dataID", 0);
		stuipdFEInsightGarabage.put("feData", new HashMap());
		stuipdFEInsightGarabage.put("insightID", insightObj.getInsightId());
		stuipdFEInsightGarabage.put("newColumns", new HashMap());
		stuipdFEInsightGarabage.put("newInsights", new Object[0]);
		stuipdFEInsightGarabage.put("pkqlData", new Object[0]);
		insightMap.put("insights", new Object[] { stuipdFEInsightGarabage });
		
		
		// we have some old legacy stuff...
		// just run and return the object
		OldInsightProcessor processor = new OldInsightProcessor((OldInsight) insightObj);
		Map<String, Object> obj = processor.runWeb();
		obj.put("isPkqlRunnable", false);
		obj.put("recipe", new Object[0]);
		obj.put("pkqlOutput", insightMap);

		// update the solr universal view count
		GlobalInsightCountUpdater.getInstance().addToQueue(projectId, insightId);

		return new NounMetadata(obj, PixelDataType.MAP, PixelOperationType.OLD_INSIGHT);
	}

	/**
	 * Get the params for the method
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
