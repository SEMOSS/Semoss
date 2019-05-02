package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import prerna.engine.impl.web.WebScrapeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetNumTableReactor extends AbstractReactor {

	public GetNumTableReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "aliasMap"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();

		String url = this.keyValue.get(this.keysToGet[0]);
		HashMap aliasMap = (HashMap)this.getNounStore().getNoun("aliasMap").get(0);

		
		WebScrapeEngine engine = new WebScrapeEngine();
		
		int numTables = engine.getNumTables(url, aliasMap);
				
		
		return new NounMetadata(numTables, PixelDataType.CONST_STRING);
	}

}
