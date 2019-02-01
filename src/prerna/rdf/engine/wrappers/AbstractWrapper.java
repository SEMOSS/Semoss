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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngineWrapper;
import prerna.engine.api.IRemoteQueryable;

public abstract class AbstractWrapper implements IRemoteQueryable, IEngineWrapper {

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

	@Override
	public void setQuery(String query) {
		LOGGER.debug("Setting the query " + query);
		this.query = query;
	}
	
	@Override
	public String getQuery() {
		return this.query;
	}
	
	@Override
	public void setEngine(IEngine engine) {
		LOGGER.debug("Set the engine to " + engine.getEngineId());
		this.engine = engine;
	}

}
