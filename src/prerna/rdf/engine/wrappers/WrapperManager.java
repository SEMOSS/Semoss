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

import org.apache.log4j.Logger;

import prerna.ds.RawGemlinSelectWrapper;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.json.JsonWrapper;
import prerna.engine.impl.json.JsonWrapper2;
import prerna.engine.impl.web.WebWrapper;
import prerna.query.interpreters.GremlinInterpreter;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.evaluator.QueryStructExpressionIterator;

public class WrapperManager {

	// main job of this class is to take an engine
	// find the type of the engine
	// and the type of query
	// and then give back a wrapper
	// I need to make this completely through reflection
	// I will do that later

	private static final Logger LOGGER = Logger.getLogger(WrapperManager.class);
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
	public IRawSelectWrapper getRawWrapper(IEngine engine, SelectQueryStruct qs) {
		IRawSelectWrapper returnWrapper = null;
		boolean genQueryString = true;
		switch(engine.getEngineType()) {
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
		case WEB : {
			returnWrapper = new WebWrapper();
			break;
		}
		case TINKER : {
			genQueryString = false;
			// since we dont do math on gremlin
			// right now, we will just construct and return a QSExpressionIterator
			GremlinInterpreter interpreter = (GremlinInterpreter) engine.getQueryInterpreter();
			interpreter.setQueryStruct(qs);
			RawGemlinSelectWrapper gdi = new RawGemlinSelectWrapper(interpreter, qs);
			gdi.execute();
			returnWrapper = new QueryStructExpressionIterator(gdi, qs);
			returnWrapper.execute();
			break;
		}
		case DATASTAX_GRAPH : {
			genQueryString = false;
			// since we dont do math on gremlin
			// right now, we will just construct and return a QSExpressionIterator
			GremlinInterpreter interpreter = (GremlinInterpreter) engine.getQueryInterpreter();
			interpreter.setQueryStruct(qs);
			RawGemlinSelectWrapper gdi = new RawGemlinSelectWrapper(interpreter, qs);
			gdi.execute();
			returnWrapper = new QueryStructExpressionIterator(gdi, qs);
			returnWrapper.execute();
			break;
		}
		case REMOTE_SEMOSS : {
			// TODO >>>timb: REST - either replace with rest remote or remove this
		}
		default: {
			//TODO: build iterator
			break;
		}
		}

		if(genQueryString) {
			long start = System.currentTimeMillis();
			IQueryInterpreter interpreter = engine.getQueryInterpreter();
			interpreter.setQueryStruct(qs);
			String query = interpreter.composeQuery();
			LOGGER.debug("Executing query on engine " + engine.getEngineId());
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
			long end = System.currentTimeMillis();
			LOGGER.info("Engine execution time = " + (end-start) + "ms");
		} 

		return returnWrapper;
	}

	// TODO >>>timb: REST - here add another engine type REMOTE or REST
	public IRawSelectWrapper getRawWrapper(IEngine engine, String query) {
		IRawSelectWrapper returnWrapper = null;
		switch(engine.getEngineType()) {
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
		case WEB : {
			returnWrapper = new WebWrapper();
			break;
		}
		case REMOTE_SEMOSS : {
			// TODO >>>timb: REST - either replace with rest remote or remove this
			//TODO: build iterator
			break;
		}
		default: {

		}
		}

		LOGGER.debug(returnWrapper.getClass() + " executing query: " + query);
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
	public ISelectWrapper getSWrapper(IEngine engine, String query) {
		ISelectWrapper returnWrapper = null;
		switch(engine.getEngineType()) {
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

		LOGGER.debug(returnWrapper.getClass() + " executing query: " + query);
		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		returnWrapper.execute();
		returnWrapper.getDisplayVariables();
		returnWrapper.getPhysicalVariables();
		return returnWrapper;
	}

	@Deprecated
	public IConstructWrapper getCWrapper(IEngine engine, String query) {
		IConstructWrapper returnWrapper = null;
		switch(engine.getEngineType())
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
		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		returnWrapper.execute();
		return returnWrapper;
	}

	@Deprecated
	public IConstructWrapper getChWrapper(IEngine engine, String query) {
		IConstructWrapper returnWrapper = null;
		switch(engine.getEngineType())
		{
		case SESAME : {
			returnWrapper = new SesameSelectCheater();
			break;
		}
		case JENA : {
			returnWrapper = new JenaSelectCheater();
			break;
		}
		case SEMOSS_SESAME_REMOTE : {
			returnWrapper = new RemoteSesameSelectCheater();
			break;
		}
		case RDBMS : {
			returnWrapper = new RDBMSSelectCheater();
			break;
		}
		default: {

		}
		}
		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		returnWrapper.execute();
		return returnWrapper;
	}

}
