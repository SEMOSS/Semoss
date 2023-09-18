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
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.ds.RawGemlinSelectWrapper;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.json.JsonWrapper;
import prerna.engine.impl.json.JsonWrapper2;
import prerna.engine.impl.web.WebWrapper;
import prerna.om.ThreadStore;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.evaluator.QueryStructExpressionIterator;
import prerna.usertracking.UserQueryTrackingThread;
import prerna.util.Constants;

public class WrapperManager {

	// main job of this class is to take an engine
	// find the type of the engine
	// and the type of query
	// and then give back a wrapper
	// I need to make this completely through reflection
	// I will do that later

	private static final Logger classLogger = LogManager.getLogger(WrapperManager.class);
	private static WrapperManager manager = null;
	private static List<String> ignoreDatabases = new ArrayList<>();
	{
		ignoreDatabases.add(Constants.LOCAL_MASTER_DB);
		ignoreDatabases.add(Constants.LOCAL_MASTER_DB + "_" + Constants.OWL_ENGINE_SUFFIX);
		
		ignoreDatabases.add(Constants.SECURITY_DB);
		ignoreDatabases.add(Constants.SECURITY_DB + "_" + Constants.OWL_ENGINE_SUFFIX);
		
		ignoreDatabases.add(Constants.SCHEDULER_DB);
		ignoreDatabases.add(Constants.SCHEDULER_DB + "_" + Constants.OWL_ENGINE_SUFFIX);

		ignoreDatabases.add(Constants.THEMING_DB);
		ignoreDatabases.add(Constants.THEMING_DB + "_" + Constants.OWL_ENGINE_SUFFIX);
		
		ignoreDatabases.add(Constants.USER_TRACKING_DB);
		ignoreDatabases.add(Constants.USER_TRACKING_DB + "_" + Constants.OWL_ENGINE_SUFFIX);
		
		ignoreDatabases.add(Constants.OWL_TEMPORAL_ENGINE_META);
	}
	
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
		String engineId = engine.getEngineId();
		boolean ignoreQueryLogging = ignoreDatabases.contains(engineId) || engineId.endsWith(Constants.OWL_ENGINE_SUFFIX);
		
		User user = ThreadStore.getUser();
		UserQueryTrackingThread queryT = null;
		if(!ignoreQueryLogging) {
			queryT = new UserQueryTrackingThread(user, engineId);
		}
		try {
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
					classLogger.debug("Executing query on engine " + engine.getEngineId());
					returnWrapper.setEngine(engine);
					returnWrapper.setQuery(query);
					// we need to pass the qs to properly set the limit/offset
					// since R will still run even if the limit+offset is > numRows and return null entries
					((RawRSelectWrapper) returnWrapper).execute(qs);
					long end = System.currentTimeMillis();
					classLogger.debug("Engine execution time = " + (end-start) + "ms");
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
					classLogger.debug("Engine execution time = " + (end-start) + "ms");
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
					classLogger.debug("Engine execution time = " + (end-start) + "ms");
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
					classLogger.debug("Engine execution time = " + (end-start) + "ms");
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
					if(ignoreQueryLogging) {
						returnWrapper.setEngine(engine);
						returnWrapper.setQuery(query);
						if(!delayExecIfPossible) {
							classLogger.debug(User.getSingleLogginName(user) + " Running query on " + engine.getEngineId());
							returnWrapper.execute();
							
							long end = System.currentTimeMillis();
							long execTime = end - start;
							classLogger.debug(User.getSingleLogginName(user) + " Running query on " + engine.getEngineId() + " finished execution time = " + execTime + "ms");
						} else {
							classLogger.debug("Delaying query execution");
						}
					} else {
						returnWrapper.setEngine(engine);
						returnWrapper.setQuery(query);
						// set the query for tracking
						queryT.setQuery(query);
						if(!delayExecIfPossible) {
							classLogger.info(User.getSingleLogginName(user) + " Running query on " + engine.getEngineId());
							// set the start time
							queryT.setStartTimeNow();
							// run
							returnWrapper.execute();
							// set the end time
							queryT.setEndTimeNow();
							
							long end = System.currentTimeMillis();
							long execTime = end - start;
							classLogger.info(User.getSingleLogginName(user) + " Running query on " + engine.getEngineId() + " finished execution time = " + execTime + "ms");
						} else {
							classLogger.info(User.getSingleLogginName(user) + " Running query on " + engine.getEngineId() + " with delayed execution");
						}
					}
				} catch(Exception e) {
					queryT.setFailed();
					classLogger.error(Constants.STACKTRACE, e);
					error = true;
					throw e;
				} finally {
					// clean up errors 
					// if the wrapper is created
					// but not returned
					if(error && returnWrapper != null) {
						if(classLogger.isDebugEnabled()) {
							classLogger.error("Error occurred executing query on engine " + engine.getEngineId() + " with query = " + query);
						}
						try {
							returnWrapper.close();
						} catch(IOException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
				}
			}
			
			return returnWrapper;
		} finally {
			if(queryT != null && !ignoreQueryLogging && !delayExecIfPossible) {
				new Thread(queryT).start();
			}
		}
	}

	/**
	 * 
	 * @param engine
	 * @param query
	 * @return
	 * @throws Exception
	 */
	public IRawSelectWrapper getRawWrapper(IDatabaseEngine engine, String query) throws Exception {
		String engineId = engine.getEngineId();
		boolean ignoreQueryLogging = ignoreDatabases.contains(engineId) || engineId.endsWith(Constants.OWL_ENGINE_SUFFIX);
		
		User user = ThreadStore.getUser();
		UserQueryTrackingThread queryT = null;
		if(!ignoreQueryLogging) {
			queryT = new UserQueryTrackingThread(user, engineId);
		}
		try {
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

			boolean error = false;
			try {
				returnWrapper.setEngine(engine);
				returnWrapper.setQuery(query);
				classLogger.info(User.getSingleLogginName(user) + " Running query on " + engine.getEngineId());

				long start = System.currentTimeMillis();
				if(ignoreQueryLogging) {
					returnWrapper.execute();
				} else {
					// set the query for tracking
					queryT.setQuery(query);
					// set the start time
					queryT.setStartTimeNow();
					// run
					returnWrapper.execute();
					// set the end time
					queryT.setEndTimeNow();
				}
				
				long end = System.currentTimeMillis();
				long execTime = end - start;
				classLogger.info(User.getSingleLogginName(user) + " Running query on " + engine.getEngineId() + " finished execution time = " + execTime + "ms");
			} catch(Exception e) {
				queryT.setFailed();
				classLogger.error(Constants.STACKTRACE, e);
				error = true;
				throw e;
			} finally {
				// clean up errors 
				// if the wrapper is created
				// but not returned
				if(error && returnWrapper != null) {
					if(classLogger.isDebugEnabled()) {
						classLogger.error("Error occurred executing query on engine " + engine.getEngineId() + " with query = " + query);
					}
					try {
						returnWrapper.close();
					} catch(IOException e) {
						classLogger.error(Constants.STACKTRACE, e);
					}
				}
			}
			
			return returnWrapper;
		} finally {
			if(!ignoreQueryLogging && queryT != null) {
				new Thread(queryT).start();
			}
		}
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

		classLogger.debug(returnWrapper.getClass() + " executing query: " + query);
		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		try {
			returnWrapper.execute();
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
			classLogger.error(Constants.STACKTRACE, e);
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
			classLogger.error(Constants.STACKTRACE, e);
		}
		return returnWrapper;
	}

}
