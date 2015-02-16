/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.rdf.main;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

public class HttpsClient {

	public static void main(String [] args) throws ClientProtocolException, IOException, KeyManagementException, UnrecoverableKeyException, NoSuchAlgorithmException, KeyStoreException
	{
		/*SSLContextBuilder builder = new SSLContextBuilder();
	    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
	    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
	            builder.build());
	    CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(
	            sslsf).build();

	    HttpGet httpGet = new HttpGet("http://www.google.com");
	    CloseableHttpResponse response = httpclient.execute(httpGet);
	    try {
	        System.out.println(response.getStatusLine());
	        HttpEntity entity = response.getEntity();
	        EntityUtils.consume(entity);
	    }
	    finally {
	        response.close();
	    }	*/
	
	String output = "";
	String api = "http://localhost:9080/Monolith/api/engine/s-Movie_DB/streamTester";
	Hashtable params = new Hashtable();
	try
	{
		URIBuilder uri = new URIBuilder(api);
		
		System.out.println("Getting data from the API...  " + api);
		System.out.println("Prams is " + params);
		
		SSLContextBuilder builder = new SSLContextBuilder();
	    builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
	    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(
	            builder.build());
	    CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(
	            sslsf).build();

		HttpPost get = new HttpPost(api);
		if(params != null) // add the parameters
		{
			List <NameValuePair> nvps = new ArrayList <NameValuePair>();
			for(Enumeration <String> keys = params.keys();keys.hasMoreElements();)
			{
				String key = keys.nextElement();
				String value = (String)params.get(key);
				uri.addParameter(key, value);
				nvps.add(new BasicNameValuePair(key, value));
			}
			get.setEntity(new UrlEncodedFormEntity(nvps));
			//get = new HttpPost(uri.build());
		}
		
		CloseableHttpResponse response = httpclient.execute(get);
		HttpEntity entity = response.getEntity();
		
		if(entity != null)
		{
			//BufferedReader stream = new BufferedReader(new InputStreamReader(entity.getContent()));
			ObjectInputStream ois = new ObjectInputStream(entity.getContent());
			Object data = null;
			while((data = ois.readObject()) != null)
				System.out.println("Data is " + data);
		}
	}catch(Exception ex)
	{
		//connected = false;
	}
	if(output.length() == 0)
		output = null;
}
}
