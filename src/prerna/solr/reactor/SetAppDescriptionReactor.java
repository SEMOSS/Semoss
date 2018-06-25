package prerna.solr.reactor;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.solr.client.solrj.SolrServerException;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrIndexEngine;

public class SetAppDescriptionReactor extends AbstractReactor {
	
	public SetAppDescriptionReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey(), "description"};
	}

	@Override
	public NounMetadata execute() {
//		organizeKeys();
//		String appName = this.keyValue.get(this.keysToGet[0]);
//		String description = this.keyValue.get(this.keysToGet[1]);
//		
//		Map<String, Object> fieldsToModify = new HashMap<String, Object>();
//		fieldsToModify.put("app_description", description);
//		try {
//			SolrIndexEngine.getInstance().modifyApp(appName, fieldsToModify);
//			return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.APP_INFO);
//		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException | SolrServerException
//				| IOException e1) {
//			e1.printStackTrace();
//		}

		return new NounMetadata(false, PixelDataType.BOOLEAN, PixelOperationType.APP_INFO);
	}

}
