/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.rdf.engine.wrappers;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.google.gson.Gson;

import prerna.algorithm.api.SemossDataType;
import prerna.cluster.util.ClusterUtil;
import prerna.cluster.util.ZKClient;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.IRemoteQueryable;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;
import prerna.util.gson.IHeadersDataRowAdapter;

public abstract class AbstractRESTWrapper implements IRemoteQueryable, IEngineWrapper, IRawSelectWrapper {

	private static final Logger LOGGER = LogManager.getLogger(AbstractWrapper.class.getName());
	
	protected transient IEngine engine = null;
	protected transient String query = null;

	/*
	 * Remote queryable class variables
	 */
	protected String id = null;
	protected String api = null;
	protected boolean remote = false;
	
	/*
	 * Engine wrapper class variables
	 */
	protected String[] headers = null;
	protected String[] rawHeaders = null;
	//TODO: move to pixel data type
	protected SemossDataType[] types = null;
	protected int numColumns;
	
	protected long numRows;

	/*
	 * Cluster variables
	 */
	private static final Gson GSON = GsonUtility.getDefaultGson();
	
	private String host = null;
	
	// What keeps this object straight on the service side
	private final String wrapperId = Utility.getRandomString(12);
		
	/*
	 * End cluster variables
	 */
	// TODO >>>timb: remove this remote API stuff / interface for it if it is not needed 
	@Override
	public void setRemoteId(String id) {
		this.id = id;
	}

	@Override
	public String getRemoteId() {
		return this.id;
	}

	@Override
	public void setRemoteAPI(String api) {
		this.api = api;
	}

	@Override
	public String getRemoteAPI() {
		return this.api;
	}
	
	@Override
	public void setRemote(boolean remote) {
		this.remote = remote;
	}
	
	@Override
	public boolean isRemote() {
		return this.remote;
	}

	//////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////// Code for cluster ////////////////////////////////////
	
	// Need to to pass in engine rather than using the class variable,
	// as setEngine is often called when the id is null.
	private boolean wrapperIsInherentlyLocal(IEngine engine) {
		String appId = engine.getEngineId();
		
		// TODO >>>timb: right now, only RDBMS works for remote
		if (engine.getEngineType() != IEngine.ENGINE_TYPE.RDBMS || appId.startsWith("security") || appId.startsWith("LocalMasterDatabase") || appId.startsWith("form_builder_engine")) {
			return true;
		} else {
			return false;
		}
	}
	
	private boolean isLocal() {
		
		// TODO >>>timb: is this a permanent fix?
		if (engine == null) {
			return true;
		} else {
			return isLocal(engine);
		}
	}
	
	private boolean isLocal(IEngine engine) {
		 return wrapperIsInherentlyLocal(engine) || ClusterUtil.LOAD_ENGINES_LOCALLY;
	}
	
	private String getHostForDB(String appId) throws KeeperException, InterruptedException {
		if (host == null) {
			host = "http://" + ZKClient.getInstance().getHostForDB(appId) + "/Monolith/api/cluster/";
		}
		return host;
	}
		
	private boolean resetHostForDB(String appId) throws KeeperException, InterruptedException {
		String oldHost = host;
		host = null;
		host = getHostForDB(appId);
		return !oldHost.equals(host);
	}
	
	/*
	 * Lazy load the host
	 * Try to make the post request
	 * If the post request fails, try reloading host, as it may have changed
	 */
	private String post(String action, String appId, List<NameValuePair> params) throws Exception {
				
		// Setup the host
		String host = getHostForDB(appId);
		HttpClient httpclient = HttpClients.createDefault();
		HttpPost httppost = getHttpPost(host, action, params);
		
		// Try to execute the call
		HttpResponse response = null;
		try {
			response = httpclient.execute(httppost);
		} catch (IOException e) {
			// TODO >>>timb: maybe use the alive here
			// If it fails, the host may have changed, so check once
			if (resetHostForDB(appId)) {
				String newHost = getHostForDB(appId);
				HttpPost newHttppost = getHttpPost(newHost, action, params);
				response = httpclient.execute(newHttppost);
			} else {
				
				// The host hasn't changed, nothing we can do
				// Just unable to connect
				// So rethrow e
				throw e;
			}
		}
		HttpEntity entity = response.getEntity();
		if (entity != null) {
		    try (InputStream instream = entity.getContent()) {
		    	 String responseString = IOUtils.toString(instream);
		    	 return responseString;
		    }
		}
		return null;
	}
	
	private HttpPost getHttpPost(String host, String action, List<NameValuePair> params) throws UnsupportedEncodingException {
		HttpPost httppost = new HttpPost(host + action);
		httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
		httppost.setHeader("Content-Type", "application/x-www-form-urlencoded");
		return httppost;
	}
	
	private String sendAction(String action) throws Exception {
		return sendAction(engine, action);
	}
	
	private String sendAction(IEngine engine, String action) throws Exception {
		String appId = engine.getEngineId();
		
		List<NameValuePair> params = new ArrayList<NameValuePair>(1);
		params.add(new BasicNameValuePair("wrapperId", wrapperId));
		params.add(new BasicNameValuePair("appId", appId));
		params.add(new BasicNameValuePair("query", query));
		return post(action, appId, params);
	}
	
	@Override
	public void execute() {
		if (isLocal()) {
			localExecute();
		} else  {
			try {
				String result = sendAction("execute");
			  	LOGGER.info(result);
			} catch (Exception e) {
				LOGGER.error("Unable to execute remote wrapper with wrapper id = " + wrapperId, e);
			}
		}
	}
	
	protected abstract void localExecute();

	@Override
	public void setQuery(String query) {
	  	this.query = query;
		if (!isLocal()) {
			try {
				String result = sendAction("setQuery");
			  	LOGGER.info(result);
			} catch (Exception e) {
				LOGGER.error("Unable to set query for remote wrapper with wrapper id = " + wrapperId, e);
			}
		}
	}
	@Override
	public String getQuery() {
		return this.query;
	}
	
	@Override
	public void setEngine(IEngine engine) {
		this.engine = engine;
		if (!isLocal(engine)) {
			try {
				String result = sendAction(engine, "setEngine");
			  	LOGGER.info(result);
			} catch (Exception e) {
				LOGGER.error("Unable to set engine for remote wrapper with wrapper id = " + wrapperId, e);
			}
		}
	}
	
	@Override
	public void cleanUp() {
		if (isLocal()) {
			localCleanUp();
		} else {
			try {
				String result = sendAction("cleanUp");
			  	LOGGER.info(result);
			} catch (Exception e) {
				LOGGER.error("Unable to clean up remote wrapper with wrapper id = " + wrapperId, e);
			}
		}		
	}
	
	protected abstract void localCleanUp();

	@Override
	public boolean hasNext() {
		if (isLocal()) {
			return localHasNext();
		} else {
			try {
				String result = sendAction("hasNext");
			  	LOGGER.info(result);
				return Boolean.parseBoolean(result);
			} catch (Exception e) {
				LOGGER.error("Unable to determine has next for remote wrapper with wrapper id = " + wrapperId, e);
				return false;
			}
		}
	}

	protected abstract boolean localHasNext();
	
	@Override
	public IHeadersDataRow next() {
		if (isLocal()) {
			return localNext();
		} else {
			try {
				String result = sendAction("next");
			  	LOGGER.info(result);
			  	IHeadersDataRowAdapter adapter = new IHeadersDataRowAdapter();
			  	IHeadersDataRow row = adapter.fromJson(result);
			  	return row;
			} catch (Exception e) {
				LOGGER.error("Unable to retrieve next row for remote wrapper with wrapper id = " + wrapperId, e);
				return null;
			}
		}
	}
	
	protected abstract IHeadersDataRow localNext();

	@Override
	public String[] getHeaders() {
		if (isLocal()) {
			return localGetHeaders();
		} else {
			// TODO execute via rest
			return null;
		}
	}
	
	protected abstract String[] localGetHeaders();

	@Override
	public SemossDataType[] getTypes() {
		if (isLocal()) {
			return localGetTypes();
		} else {
			// TODO execute via rest
			return null;
		}
	}
	
	protected abstract SemossDataType[] localGetTypes();

	@Override
	public long getNumRecords() {
		if (isLocal()) {
			return localGetNumRecords();
		} else {
			// TODO execute via rest
			return 0;
		}
	}
	
	protected abstract long localGetNumRecords();
	
	@Override
	public long getNumRows() {
		if (isLocal()) {
			return localGetNumRows();
		} else {
			// TODO execute via rest
			return 0;
		}
	}

	protected abstract long localGetNumRows();
	
	@Override
	public void reset() {
		if (isLocal()) {
			localReset();
		} else {
			// TODO execute via rest	
		}		
	}
	
	protected abstract void localReset();
	
}
