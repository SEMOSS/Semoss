package prerna.sablecc2.reactor.masterdatabase;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;

import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;

public class DatabaseInfoReactor extends AbstractReactor {
	



	public DatabaseInfoReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.ENGINE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		String searchString = "*:*";
		int groupOffsetInt = 0;
		int groupLimitInt = 5;
		String groupByField = "core_engine";
		String groupSort = "view_count";
		Map<String, List<String>> filterData = new HashMap<String, List<String>>();
		
		Map<String, Object> groupFieldMap = null;
		try {
			groupFieldMap = SolrIndexEngine.getInstance().executeQueryGroupBy(searchString, groupOffsetInt, groupLimitInt, groupByField, groupSort, filterData);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException | IOException e) {
			e.printStackTrace();
		}

		return new NounMetadata(groupFieldMap, PixelDataType.CUSTOM_DATA_STRUCTURE);
	}

}
