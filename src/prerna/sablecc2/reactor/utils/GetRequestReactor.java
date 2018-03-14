package prerna.sablecc2.reactor.utils;

import java.io.IOException;

import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

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
		
		CloseableHttpClient httpClient = HttpClients.createDefault();
		HttpGet httpGet = new HttpGet(url);
		
		ResponseHandler<String> handler = new BasicResponseHandler();
		CloseableHttpResponse response;
		try {
			response = httpClient.execute(httpGet);
		} catch (IOException e1) {
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
