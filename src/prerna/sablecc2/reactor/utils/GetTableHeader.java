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

public class GetTableHeader extends AbstractReactor {

	public GetTableHeader() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "alias_map"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		HashMap aliasMap = (HashMap)this.getNounStore().getNoun("aliasMap").get(0);
		
		WebScrapeEngine engine = new WebScrapeEngine();
		
		String [] headers = engine.getHeaders(url, aliasMap);
				
		StringBuffer accumulator = new StringBuffer("");
		
		for(int headerIndex = 0;headerIndex < headers.length;headerIndex++)
		{
			if(headerIndex != 0)
				accumulator.append("||");
			accumulator.append(headers[headerIndex]);
		}
		accumulator.append("");
		
		return new NounMetadata(accumulator.toString(), PixelDataType.CONST_STRING);
	}

}
