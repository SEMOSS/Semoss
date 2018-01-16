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

import prerna.ds.QueryStruct;
import prerna.ds.TinkerHeadersDataRowIterator2;
import prerna.ds.datastax.DataStaxGraphEngine;
import prerna.ds.datastax.DataStaxGraphIterator;
import prerna.ds.datastax.DataStaxInterpreter;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.solr.SolrEngine;
import prerna.engine.impl.solr.SolrIterator;
import prerna.engine.impl.tinker.TinkerEngine;
import prerna.query.interpreters.GremlinInterpreter2;
import prerna.query.interpreters.IQueryInterpreter2;
import prerna.query.interpreters.SolrInterpreter2;
import prerna.query.querystruct.QueryStruct2;
import prerna.query.querystruct.evaluator.QueryStructExpressionIterator;
import prerna.rdf.query.builder.IQueryInterpreter;

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
	
	public IRawSelectWrapper getRawWrapper(IEngine engine, QueryStruct2 qs) {
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
//		case SEMOSS_SESAME_REMOTE : {
//			//TODO: need to build out RemoteSesameSelectWrapper
//			/*System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
//				System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
//				System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
//				System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
//			 */
//			returnWrapper = new RemoteSesameSelectWrapper();
//			break;
//		}
		case TINKER : {
			// since we dont do math on gremlin
			// right now, we will just construct and return a QSExpressionIterator
			GremlinInterpreter2 interpreter = new GremlinInterpreter2( ((TinkerEngine) engine).getGraph());
			interpreter.setQueryStruct(qs);
			return new QueryStructExpressionIterator(new TinkerHeadersDataRowIterator2(interpreter.composeIterator(), qs), qs);
		}
		case DATASTAXGRAPH : {
			// since we dont do math on gremlin
			// right now, we will just construct and return a QSExpressionIterator
			DataStaxInterpreter interpreter = new DataStaxInterpreter( ((DataStaxGraphEngine) engine).getGraphTraversalSource());
			interpreter.setQueryStruct(qs);
			return new QueryStructExpressionIterator(new DataStaxGraphIterator(interpreter.composeIterator(), qs), qs);
		}
		case R : {
			returnWrapper = new RawRSelectWrapper();
			break;
		}

		case SOLR : {
			SolrInterpreter2 solrInterp = new SolrInterpreter2();
			solrInterp.setQueryStruct(qs);
			SolrEngine solrEngine = (SolrEngine) engine;
			SolrIterator it = new SolrIterator(solrEngine.execSolrQuery(solrInterp.composeSolrQuery()), qs);
			return new QueryStructExpressionIterator(it, qs);
		}
		default: {

		}
		}

		if(genQueryString) {
			long start = System.currentTimeMillis();
			IQueryInterpreter2 interpreter = engine.getQueryInterpreter2();
			interpreter.setQueryStruct(qs);
			String query = interpreter.composeQuery();
			LOGGER.debug("Executing query on engine " + engine.getEngineName());
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
			long end = System.currentTimeMillis();
			LOGGER.info("Engine execution time = " + (end-start) + "ms");
		} 
//		else {
//			returnWrapper.setEngine(engine);
//			returnWrapper.setQueryStruct(qs);
//			returnWrapper.execute();
//		}

		return returnWrapper;
	}
	
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
//		case SEMOSS_SESAME_REMOTE : {
//			//TODO: need to build out RemoteSesameSelectWrapper
//			/*System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
//			System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
//			System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
//			System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
//			 */
//			returnWrapper = new RemoteSesameSelectWrapper();
//			break;
//		}
//		case TINKER : {
//			returnWrapper = new RawTinkerSelectWrapper();
//			break;
//		}
		default: {

		}
		}

		LOGGER.debug(returnWrapper.getClass() + " executing query: " + query);
		returnWrapper.setEngine(engine);
		returnWrapper.setQuery(query);
		returnWrapper.execute();

		return returnWrapper;
	}

	@Deprecated
	public IRawSelectWrapper getRawWrapper(IEngine engine, QueryStruct qs) {
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
		case SEMOSS_SESAME_REMOTE : {
			//TODO: need to build out RemoteSesameSelectWrapper
			/*System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
				System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
				System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
				System.err.println("NEED TO IMPLEMENT RAW QUERY FOR REMOTE SESAME SELECT WRAPPER!!!!!");
			 */
			returnWrapper = new RemoteSesameSelectWrapper();
			break;
		}
//		case TINKER : {
//			genQueryString = false;
//			returnWrapper = new RawTinkerSelectWrapper();
//			break;
//		}
//		case SOLR : {
//			genQueryString = false;
//			returnWrapper = new RawSolrSelectWrapper();
//		}
		default: {

		}
		}

		if(genQueryString) {
			IQueryInterpreter interpreter = engine.getQueryInterpreter();
			interpreter.setQueryStruct(qs);
			String query = interpreter.composeQuery();
			LOGGER.debug("Executing query on engine " + engine.getEngineName());
			returnWrapper.setEngine(engine);
			returnWrapper.setQuery(query);
			returnWrapper.execute();
		} else {
			returnWrapper.setEngine(engine);
//			returnWrapper.setQueryStruct(qs);
			returnWrapper.execute();
		}

		return returnWrapper;
	}

	public ISelectWrapper getSWrapper(IEngine engine, String query)
	{
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
			//TBD
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

	public IConstructWrapper getCWrapper(IEngine engine, String query)
	{
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

	public IConstructWrapper getChWrapper(IEngine engine, String query)
	{
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
		//ISelectWrapper doh = (ISelectWrapper)returnWrapper;
		//returnWrapper.getVariables();

		return returnWrapper;
	}

}
