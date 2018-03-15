package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetRequestReactor extends AbstractReactor {

	public GetRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		
		ResponseHandler<String> handler = new BasicResponseHandler();
		CloseableHttpResponse response = null;
		try {
			SSLContextBuilder builder = new SSLContextBuilder();
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
			CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
			HttpGet httpGet = new HttpGet(url);
			response = httpClient.execute(httpGet);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} catch (KeyStoreException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} catch (KeyManagementException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
		
		String retString = null;
		try {
			retString = handler.handleResponse(response);
		} catch (IOException e) {
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
		
		return new NounMetadata(retString, PixelDataType.CONST_STRING);
	}

}
