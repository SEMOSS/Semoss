/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.rdf.engine.wrappers;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.IEngineWrapper;
import prerna.rdf.engine.api.IRemoteQueryable;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;

public abstract class AbstractWrapper implements IRemoteQueryable, IEngineWrapper{

	String ID = null;
	String api = null;
	boolean remote = false;
	static final Logger logger = LogManager.getLogger(SesameJenaSelectWrapper.class.getName());
	transient IEngine engine = null;
	transient Enum engineType;
	transient String query = null;
	transient String [] var = null;


	
	@Override
	public void setRemoteID(String id) {
		// TODO Auto-generated method stub
		this.ID = id;
	}

	@Override
	public String getRemoteID() {
		// TODO Auto-generated method stub
		return this.ID;
	}

	@Override
	public void setRemoteAPI(String engine) {
		// TODO Auto-generated method stub
		this.api = engine;
	}

	@Override
	public String getRemoteAPI() {
		// TODO Auto-generated method stub
		return this.api;
	}
	
	public void setRemote(boolean remote)
	{
		this.remote = remote;
	}

	@Override
	public void setQuery(String query) {
		logger.debug("Setting the query " + query);
		this.query = query;
	}

	@Override
	public void setEngine(IEngine engine) {
		logger.debug("Set the engine " );
		this.engine = engine;
		if(engine == null) engineType = IEngine.ENGINE_TYPE.JENA;
		else engineType = engine.getEngineType();
	}

	public abstract void execute();

	public abstract boolean hasNext();

}
