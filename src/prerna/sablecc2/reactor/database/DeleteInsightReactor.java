package prerna.sablecc2.reactor.database;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.SolrServerException;

import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DeleteInsightReactor extends AbstractReactor {

	public DeleteInsightReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.INSIGHT_ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineName = this.keyValue.get(this.keysToGet[0]);
		List<String> insightIDList = new ArrayList<String>(Arrays.asList(getInsightIDs()));
		List<String> solrIDList = new ArrayList<String>();
		IEngine engine = Utility.getEngine(engineName);
		
		// delete from insights database
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		try {
			admin.dropInsight(insightIDList.toArray(new String[insightIDList.size()]));
			solrIDList = SolrIndexEngine.getSolrIdFromInsightEngineId(engine.getEngineName(), insightIDList);
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
		
		// delete from solr
		if (solrIDList != null) {
			SolrIndexEngine solrE;
			try {
				solrE = SolrIndexEngine.getInstance();
				if (solrE.serverActive()) {
					solrE.removeInsight(solrIDList);
				}
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (KeyManagementException e) {
				e.printStackTrace();
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			} catch (KeyStoreException e) {
				e.printStackTrace();
			}
		}	

		// delete insight .mosfet file, if it exists
		String recipePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\" + Constants.DB + "\\" + engineName + "\\version\\" ;
		for (int i = 0 ; i < insightIDList.size() ; i++){
			File dir = new File(recipePath +  insightIDList.get(i));
			
			// if it exists then delete all files inside
			if (dir.exists() && dir.isDirectory()){
				try {
					FileUtils.deleteDirectory(dir);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_INSIGHT);
	}
	
	private String[] getInsightIDs() {
		GenRowStruct insightIDsGrs = this.store.getNoun(ReactorKeysEnum.INSIGHT_ID.getKey());
		if (insightIDsGrs.size() > 0) {
			String[] ids = new String[insightIDsGrs.size()];
			for (int i = 0; i < insightIDsGrs.size(); i++) {
				String id = insightIDsGrs.get(i).toString();
				ids[i] = id;
			}
			return ids;
		}
		throw new IllegalArgumentException("Need to define insight IDs to delete.");
	}

}
	