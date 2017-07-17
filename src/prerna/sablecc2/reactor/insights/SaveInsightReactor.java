package prerna.sablecc2.reactor.insights;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.solr.SolrIndexEngine;
import prerna.util.Utility;

public class SaveInsightReactor extends AbstractInsightReactor {

	private static final Logger LOGGER = Logger.getLogger(SaveInsightReactor.class.getName());
	
	@Override
	public NounMetadata execute() {
		// get the recipe for the insight
		// need the engine name and id that has the recipe
		String engineName = getEngine();
		String insightName = getInsightName();
		// not sure how to do the layout piece of this.....
		
		IEngine coreEngine = Utility.getEngine(engineName);
		// add the recipe to the insights database	
		InsightAdministrator admin = new InsightAdministrator(coreEngine.getInsightDatabase());
		
		LOGGER.info("1) Add insight to rdbms");
		String newRdbmsId = admin.addInsight(insightName, "grid", this.insight.getPkslRecipe().toArray(new String[]{}));
		LOGGER.info("1) Done");

		LOGGER.info("2) Add insight to solr");
		addNewInsightToSolr(engineName, newRdbmsId, insightName, "grid", "", new ArrayList<String>(), "");
		LOGGER.info("2) Done");

		Map<String, Object> returnMap = new HashMap<String, Object>();
		returnMap.put("insightId", newRdbmsId);
		returnMap.put("engine", engineName);
		returnMap.put("success", true);
		NounMetadata noun = new NounMetadata(returnMap, PkslDataTypes.CUSTOM_DATA_STRUCTURE);
		return noun;
	}
	
	/**
	 * Add an insight into solr
	 * @param insightIdToSave
	 * @param insightName
	 * @param layout
	 * @param description
	 * @param tags
	 * @param userId
	 */
	private void addNewInsightToSolr(String engineName, String insightIdToSave, String insightName, String layout, String description, List<String> tags, String userId) {
		Map<String, Object> solrInsights = new HashMap<>();
		DateFormat dateFormat = SolrIndexEngine.getDateFormat();
		Date date = new Date();
		String currDate = dateFormat.format(date);
		solrInsights.put(SolrIndexEngine.STORAGE_NAME, insightName);
		solrInsights.put(SolrIndexEngine.TAGS, tags);
		solrInsights.put(SolrIndexEngine.LAYOUT, layout);
		solrInsights.put(SolrIndexEngine.CREATED_ON, currDate);
		solrInsights.put(SolrIndexEngine.MODIFIED_ON, currDate);
		solrInsights.put(SolrIndexEngine.LAST_VIEWED_ON, currDate);
		solrInsights.put(SolrIndexEngine.DESCRIPTION, description);
		solrInsights.put(SolrIndexEngine.CORE_ENGINE_ID, Integer.parseInt(insightIdToSave));
		solrInsights.put(SolrIndexEngine.USER_ID, userId);

		// TODO: figure out which engines are used within this insight
		solrInsights.put(SolrIndexEngine.CORE_ENGINE, engineName);
		solrInsights.put(SolrIndexEngine.ENGINES, new HashSet<String>().add(engineName));

		// the image will be updated in a later thread
		// for now, just send in an empty string
		solrInsights.put(SolrIndexEngine.IMAGE, "");
		try {
			SolrIndexEngine.getInstance().addInsight(engineName + "_" + insightIdToSave, solrInsights);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e1) {
			e1.printStackTrace();
		}
	}
	

}
