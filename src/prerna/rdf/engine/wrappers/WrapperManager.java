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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.ds.RawGemlinSelectWrapper;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.json.JsonWrapper;
import prerna.engine.impl.json.JsonWrapper2;
import prerna.engine.impl.web.WebWrapper;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.evaluator.QueryStructExpressionIterator;
import prerna.util.Constants;
import prerna.util.Utility;

public class WrapperManager {

	// main job of this class is to take an engine
	// find the type of the engine
	// and the type of query
	// and then give back a wrapper
	// I need to make this completely through reflection
	// I will do that later

	private static final Logger logger = LogManager.getLogger(WrapperManager.class);
	private static WrapperManager manager = null;

	private WrapperManager() {

	}

	public static WrapperManager getInstance() {
		// cant get lazier than this :)
		if(manager == null) {
			manager = new WrapperManager();
		}
		return manager;
	}
	
	// TODO >>>timb: REST - here add another engine type REMOTE or REST
	public IRawSelectWrapper getRawWrapper(IDatabaseEngine engine, SelectQueryStruct qs) throws Exception {
		return getRawWrapper(engine, qs, false);
	}

	public IRawSelectWrapper getRawWrapper(IDatabaseEngine engine, SelectQueryStruct qs, boolean delayExecIfPossible) throws Exception {
		IRawSelectWrapper returnWrapper = null;
		boolean genQueryString = true;
		switch(engine.getDatabaseType()) {
			case SESAME : {
				returnWrapper = new RawSesameSelectWrapper();
				break;
			}
			case JENA : {
				returnWrapper = new RawJenaSelectWrapper();
				break;
			}
			case RDBMS : {
				returnWrapper = new RawRDBMSSelectWrapper();
				break;
			}
			case IMPALA : {
				returnWrapper = new RawImpalaSelectWrapper(qs);
				break;
			}
			case R : {
				long start = System.currentTimeMillis();
				genQueryString = false;
				returnWrapper = new RawRSelectWrapper();
				IQueryInterpreter interpreter = engine.getQueryInterpreter();
				interpreter.setQueryStruct(qs);
				String query = interpreter.composeQuery();
				logger.debug("Executing query on engine " + engine.getEngineId());
				returnWrapper.setEngine(engine);
				returnWrapper.setQuery(query);
				// we need to pass the qs to properly set the limit/offset
				// since R will still run even if the limit+offset is > numRows and return null entries
				((RawRSelectWrapper) returnWrapper).execute(qs);
				long end = System.currentTimeMillis();
				logger.debug("Engine execution time = " + (end-start) + "ms");
				break;
			}
			case JSON : {
				returnWrapper = new JsonWrapper();
				break;
			}
			case JSON2 : {
				returnWrapper = new JsonWrapper2();
				break;
			}
	//		case JMES_API : {
	//			returnWrapper = new JmesWrapper();
	//			break;
	//		}
			case WEB : {
				returnWrapper = new WebWrapper();
				break;
			}
			case TINKER : {
				long start = System.currentTimeMillis();
				genQueryString = false;
				// since we dont do math on gremlin
				// right now, we will just construct and return a QSExpressionIterator
				GremlinInterpreter interpreter = (GremlinInterpreter) engine.getQueryInterpreter();
				interpreter.setQueryStruct(qs);
				RawGemlinSelectWrapper gdi = new RawGemlinSelectWrapper(interpreter, qs);
				gdi.execute();
				returnWrapper = new QueryStructExpressionIterator(gdi, qs);
				returnWrapper.execute();
				long end = System.currentTimeMillis();
				logger.debug("Engine execution time = " + (end-start) + "ms");
				break;
			}
			case JANUS_GRAPH : {
				long start = System.currentTimeMillis();
				genQueryString = false;
				// since we dont do math on gremlin
				// right now, we will just construct and return a QSExpressionIterator
				GremlinInterpreter interpreter = (GremlinInterpreter) engine.getQueryInterpreter();
				interpreter.setQueryStruct(qs);
				RawGemlinSelectWrapper gdi = new RawGemlinSelectWrapper(interpreter, qs);
				gdi.execute();
				returnWrapper = new QueryStructExpressionIterator(gdi, qs);
				returnWrapper.execute();
				long end = System.currentTimeMillis();
				logger.debug("Engine execution time = " + (end-start) + "ms");
				break;
			}
			case DATASTAX_GRAPH : {
				long start = System.currentTimeMillis();
				genQueryString = false;
				// since we dont do math on gremlin
				// right now, we will just construct and return a QSExpressionIterator
				GremlinInterpreter interpreter = (GremlinInterpreter) engine.getQueryInterpreter();
				interpreter.setQueryStruct(qs);
				RawGemlinSelectWrapper gdi = new RawGemlinSelectWrapper(interpreter, qs);
				gdi.execute();
				returnWrapper = new QueryStructExpressionIterator(gdi, qs);
				returnWrapper.execute();
				long end = System.currentTimeMillis();
				logger.debug("Engine execution time = " + (end-start) + "ms");
				break;
			}
			case REMOTE_SEMOSS : {
				// TODO >>>timb: REST - either replace with rest remote or remove this
			}
			case NEO4J_EMBEDDED : {
				returnWrapper = new Neo4jWrapper();
				break;
			}
			case NEO4J : {
				returnWrapper = new RawRDBMSSelectWrapper();
				break;
			}
			default: {
				//TODO: build iterator
				break;
			}
		}

		if (returnWrapper == null) {
			throw new NullPointerException("No wrapper has been identifier for engine of type " + engine.getDatabaseType());
		}

		if(genQueryString) {
			boolean error = false;
			String query = null;
			try {
				long start = System.currentTimeMillis();
				IQueryInterpreter interpreter = engine.getQueryInterpreter();
				interpreter.setQueryStruct(qs);
				query = interpreter.composeQuery();
				String appId = engine.getEngineId();
				if(Constants.LOCAL_MASTER_DB_NAME.equals(appId) || Constants.SECURITY_DB.equals(appId)  
						|| Constants.SCHEDULER_DB.equals(appId) || Constants.THEMING_DB.equals(appId) 
						|| Constants.USER_TRACKING_DB.equals(appId)) {
					returnWrapper.setEngine(engine);
					returnWrapper.setQuery(query);
					if(!delayExecIfPossible) {
						logger.debug("Executing query on engine " + engine.getEngineId());
						returnWrapper.execute();
						long end = System.currentTimeMillis();
						logger.debug("Engine execution time = " + (end-start) + "ms");
					} else {
						logger.debug("Delaying query execution");
					}
				} else {
					returnWrapper.setEngine(engine);
					returnWrapper.setQuery(query);
					if(!delayExecIfPossible) {
						logger.info("Executing query on engine " + engine.getEngineId());
						returnWrapper.execute();
						long end = System.currentTimeMillis();
						logger.info("Engine execution time = " + (end-start) + "ms");
					} else {
						logger.info("Delaying query execution");
					}
				}
			} catch(Exception e) {
				logger.error(Constants.STACKTRACE, e);
				error = true;
				throw e;
			} finally {
				// clean up errors 
				// if the wrapper is created
				// but not returned
				if(error && returnWrapper != null) {
					if(logger.isDebugEnabled()) {
						logger.error("Error occurred executing query on engine " + engine.getEngineId() + " with query = " + query);
					}
					returnWrapper.cleanUp();
				}
			}
		}

		return returnWrapper;
	}

	// TODO >>>timb: REST - here add another engine type REMOTE or REST
	public IRawSelectWrapper getRawWrapper(IDatabaseEngine engine, String query) throws Exception {
		IRawSelectWrapper returnWrapper = null;
		switch(engine.getDatabaseType()) {
			case SESAME : {
				returnWrapper = new RawSesameSelectWrapper();
				break;
			}
			case JENA : {
				returnWrapper = new RawJenaSelectWrapper();
				break;
			}
			case RDBMS : {
				returnWrapper = new RawRDBMSSelectWrapper();
				break;
			}
			case IMPALA : {
				returnWrapper = new RawImpalaSelectWrapper();
				break;
			}
			case R : {
				returnWrapper = new RawRSelectWrapper();
				break;
			}
			case JSON : {
				returnWrapper = new JsonWrapper();
				break;
			}
			case JSON2 : {
				returnWrapper = new JsonWrapper2();
				break;
			}
	//		case JMES_API : {
	//			returnWrapper = new JmesWrapper();
	//			break;
	//		}
			case WEB : {
				returnWrapper = new WebWrapper();
				break;
			}
			case REMOTE_SEMOSS : {
				// TODO >>>timb: REST - either replace with rest remote or remove this
				//TODO: build iterator
				break;
			}
			case NEO4J_EMBEDDED : {
				returnWrapper = new Neo4jWrapper();
				break;
			}
			case NEO4J: {
				returnWrapper = new RawRDBMSSelectWrapper();
				break;
			}
			default: {
	
			}
		}

		if (returnWrapper == null) {
			throw new NullPointerException("No wrapper has been identifier for engine of type " + engine.getDatabaseType());
		}

		logger.debug(returnWrapper.getClass() + " executing query: " + Utility.cleanLogString(query));
		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		returnWrapper.execute();

		return returnWrapper;
	}
	
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////

	/*
	 * Deprecated methods
	 */

	@Deprecated
	public ISelectWrapper getSWrapper(IDatabaseEngine engine, String query) {
		ISelectWrapper returnWrapper = null;
		switch(engine.getDatabaseType()) {
			case SESAME : {
				returnWrapper = new SesameSelectWrapper();
				break;
			}
			case JENA : {
				returnWrapper = new JenaSelectWrapper();
				break;
			}
			case SEMOSS_SESAME_REMOTE : {
				returnWrapper = new RemoteSesameSelectWrapper();
				break;
			}
			case RDBMS : {
				returnWrapper = new RDBMSSelectWrapper();
				break;
			}
			default: {
	
			}
		}

		if (returnWrapper == null) {
			throw new NullPointerException("No wrapper has been identifier for engine of type " + engine.getDatabaseType());
		}

		logger.debug(returnWrapper.getClass() + " executing query: " + query);
		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		try {
			returnWrapper.execute();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		returnWrapper.getDisplayVariables();
		returnWrapper.getPhysicalVariables();
		return returnWrapper;
	}

	@Deprecated
	public IConstructWrapper getCWrapper(IDatabaseEngine engine, String query) {
		IConstructWrapper returnWrapper = null;
		switch(engine.getDatabaseType())
		{
			case SESAME : {
				returnWrapper = new SesameConstructWrapper();
				break;
			}
			case JENA : {
				returnWrapper = new JenaConstructWrapper();
				break;
			}
			case SEMOSS_SESAME_REMOTE : {
				returnWrapper = new RemoteSesameConstructWrapper();
				break;
			}
			case RDBMS : {
				//TBD
			}
	
			default: {
	
			}
		}

		if (returnWrapper == null) {
			throw new NullPointerException("returnWrapper cannot be null here.");
		}

		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		try {
			returnWrapper.execute();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return returnWrapper;
	}

	@Deprecated
	public IConstructWrapper getChWrapper(IDatabaseEngine engine, String query) {
		IConstructWrapper returnWrapper = null;
		switch(engine.getDatabaseType())
		{
			case SESAME : {
				returnWrapper = new SesameSelectCheater();
				break;
			}
			case JENA : {
				returnWrapper = new JenaSelectCheater();
				break;
			}
//			case SEMOSS_SESAME_REMOTE : {
//				returnWrapper = new RemoteSesameSelectCheater();
//				break;
//			}
			case RDBMS : {
				returnWrapper = new RDBMSSelectCheater();
				break;
			}
			default: {
	
			}
		}

		if (returnWrapper == null) {
			throw new NullPointerException("No wrapper has been identifier for engine of type " + engine.getDatabaseType());
		}

		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		try {
			returnWrapper.execute();
		} catch (Exception e) {
			logger.error(Constants.STACKTRACE, e);
		}
		return returnWrapper;
	}

}
