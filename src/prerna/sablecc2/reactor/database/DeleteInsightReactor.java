package prerna.sablecc2.reactor.database;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Vector;

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
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		// get engine name from the id list
		String engineName = null;
		// get ids to delete
		List<String> solrIDList = new Vector<String>();
		List<String> insightIDList = new Vector<String>();
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if (grs != null && !grs.isEmpty()) {
			int size = grs.size();
			for (int i = 0; i < size; i++) {
				// id is passed in from solr id where it is defined as engine_id
				// so I need to split it
				String id = grs.get(i).toString();
				if (id.contains("_")) {
					String[] split = id.split("_");
					engineName = split[0];
					insightIDList.add(split[1]);
					solrIDList.add(id);
				}
			}
		}

		// no key is added, grab all ids
		int size = this.curRow.size();
		for (int i = 0; i < size; i++) {
			String id = this.curRow.get(i).toString();
			// id is passed in from solr id where it is defined as engine_id
			// so I need to split it
			if (id.contains("_")) {
				String[] split = id.split("_");
				engineName = split[0];
				insightIDList.add(split[1]);
				solrIDList.add(id);
			}
		}
		IEngine engine = Utility.getEngine(engineName);
		if (engine != null) {
			// delete from insights database
			InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
			try {
				admin.dropInsight(insightIDList.toArray(new String[insightIDList.size()]));
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
			String recipePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\" + Constants.DB + "\\" + engineName + "\\version\\";
			for (int i = 0; i < insightIDList.size(); i++) {
				File dir = new File(recipePath + insightIDList.get(i));
				// if it exists then delete all files inside
				if (dir.exists() && dir.isDirectory()) {
					try {
						FileUtils.deleteDirectory(dir);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.DELETE_INSIGHT);
		}
		// unable to delete
		return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.DELETE_INSIGHT);

	}

}
