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
package prerna.nameserver;

import java.sql.Connection;
import java.sql.Statement;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.Constants;
import prerna.util.Utility;

public class DeleteFromMasterDB {

	// delete from engineconcept where engine in (select id from engine where enginename='Mv1')
	// delete from enginerelation where engine in (select id from engine where enginename='Mv1')
	// delete from engine where enginename='Mv1'

	public boolean deleteEngineRDBMS(String engineName)
	{
		System.err.println("Removing engine from Local Master " + engineName);
		try {
			RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
			Connection conn = engine.makeConnection();
			Statement stmt = null;
			
			String metaDelete = "delete from conceptmetadata where physicalnameid in (select physicalnameid from engineconcept where engine ='" + engineName +"')";
			String relationDelete = "delete from enginerelation where engine ='" + engineName +"'";
			String conceptDelete = "delete from engineconcept where engine ='" + engineName +"'";
			String engineDelete = "delete from engine where id ='" + engineName +"'";
			String kvDelete = "delete from kvstore where k like '%" + engineName + "%PHYSICAL'";

			// delete metadata
			try {
				stmt = conn.createStatement();
				stmt.execute(metaDelete.toString());
			} catch(Exception e) {
				// ignore
			} finally {
				if(stmt != null) {
					stmt.close();
				}
			}
			
			// delete relation
			try {
				stmt = conn.createStatement();
				stmt.execute(relationDelete);
			} catch(Exception e) {
				// ignore
			} finally {
				if(stmt != null) {
					stmt.close();
				}
			}
			
			// delete concept
			try {
				stmt = conn.createStatement();
				stmt.execute(conceptDelete);
			} catch(Exception e) {
				// ignore
			} finally {
				if(stmt != null) {
					stmt.close();
				}
			}
			
			// delete engine
			try {
				stmt = conn.createStatement();
				stmt.execute(engineDelete);
			} catch(Exception e) {
				// ignore
			} finally {
				if(stmt != null) {
					stmt.close();
				}
			}
			
			// delete kv
			try {
				stmt = conn.createStatement();
				stmt.execute(kvDelete);
			} catch(Exception e) {
				// ignore
			} finally {
				if(stmt != null) {
					stmt.close();
				}
			}
			
			//TODO:
			// PK -> I am putting this here as a fix for this so
			// when we load a new db with this same name in the future
			// it is good
			engine.conceptIdHash = null;
		} catch(Exception ex) {
			ex.printStackTrace();
			return false;
		}
		return true;
	}
	
//	
//	public boolean deleteEngine(String engineName)
//	{
//		boolean success = false;
//		try
//		{
//			IEngine localMaster = Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
//			// delete queries are sooooooo slow :(
//			// need to store the triples and directly delete 
//			// from the rc
//
//			long getTriplesStartTime = System.currentTimeMillis();
//
//			List<Object[]> removeData = new Vector<Object[]>();
//			addEngineConceptTriplesToRemove(engineName, localMaster, removeData);
//			addAllEnginePropertyTriplesToDelete(engineName, localMaster, removeData);
//			addEngineRelationshipTriplesToDelete(engineName, localMaster, removeData);
//			addEngineMetadataToDelete(engineName, localMaster, removeData);
//
//			long getTriplesEndTime = System.currentTimeMillis();
//
//			LOGGER.info("TIME TO GET ALL TRIPLES TO REMOVE::: " + (getTriplesEndTime - getTriplesStartTime) + " ms" );
//
//
//			long removeDataStartTime = System.currentTimeMillis();
//
//			int i = 0;
//			int size = removeData.size();
//			for(; i < size; i++) {
//				localMaster.doAction(ACTION_TYPE.REMOVE_STATEMENT, removeData.get(i));
//			}
//
//			long removeDataEndTime = System.currentTimeMillis();
//
//			LOGGER.info("TIME TO DELETE ALL TRIPLES ::: " + (removeDataEndTime - removeDataStartTime) + " ms" );
//
//			success = true;
//		} catch(Exception ex) {
//			ex.printStackTrace();
//		}
//
//		return success;
//	}
//
//	/**
//	 * Query to get all the concept triples to remove
//	 * @param engineName
//	 * @param localMaster
//	 * @param deleteTriplesList
//	 */
//	private void addEngineConceptTriplesToRemove(String engineName, IEngine localMaster, List<Object[]> deleteTriplesList) {
//		// delete unique engine-concept composite subclass of stuff
//		String query = "SELECT ?conceptComposite ?subclass ?rdfConcept WHERE {"
//				+ "BIND(<" + RDFS.subClassOf + "> AS ?subclass) "
//				// unique engine-concept present in engine we are deleting
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite subclass of stuff
//				+ "{?conceptComposite ?subclass ?rdfConcept}"
////				+ "FILTER("
////				// filters for concept relationships
////				+ 	" ?subclass = <" + RDFS.subClassOf + "> "
////				+ 	")"
//				+ "}";
//		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
//			deleteTriplesList.add(data);
//		}
//
//		// delete unique engine-concept composite to its physical URI
//		query = "SELECT ?conceptComposite ?conceptType ?conceptPhysical WHERE {"
//				+ "BIND(<" + RDF.TYPE + "> AS ?conceptType) "
//				// unique engine-concept present in engine we are deleting
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite is a type of a physical uri
//				+ "{?conceptComposite ?conceptType ?conceptPhysical}"
////				+ "FILTER("
////				// filters for concept relationships
////				+ 	" ?conceptType = <" + RDF.TYPE + "> "
////				+ 	")"
//				+ "}";
//		manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//
//		// delete all logical names assigned to this engine-concept composite
//		query = "SELECT ?conceptComposite ?conceptLogicalRel ?conceptLogical WHERE {"
//				+ "BIND(<http://semoss.org/ontologies/Relation/logical> AS ?conceptLogicalRel) "
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite to its conceptual name
//				+ "{?conceptComposite ?conceptLogicalRel ?conceptLogical}"
////				+ "FILTER("
////				// filters for concept relationships
////				+ 	" ?conceptLogicalRel = <http://semoss.org/ontologies/Relation/logical> "
////				+ 	")"
//				+"}";
//		manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//
//		// delete the conceptual name for the engine-concept composite
//		query = "SELECT ?conceptComposite ?conceptConceptualRel ?conceptualName WHERE {"
//				+ "BIND(<http://semoss.org/ontologies/Relation/conceptual> AS ?conceptConceptualRel) "
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite its its logical names
//				+ "{?conceptComposite ?conceptConceptualRel ?conceptualName}"
////				+ "FILTER("
////				// filters for concept relationships
////				+ 	" ?conceptConceptualRel = <http://semoss.org/ontologies/Relation/conceptual> "
////				+ 	")"
//				+"}";
//		manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//	}
//
//	/**
//	 * Get all the property triples to delete
//	 * @param engineName
//	 * @param localMaster
//	 * @param deleteTriplesList
//	 */
//	private void addAllEnginePropertyTriplesToDelete(String engineName, IEngine localMaster, List<Object[]> deleteTriplesList) {
//		// delete the relationship between the engine-concept composite to engine-concept-property composite
//		String query = "SELECT ?conceptComposite ?prop ?propertyComposite WHERE {"
//				+ "BIND( <http://www.w3.org/2002/07/owl#DatatypeProperty> AS ?prop) "
//				// unique engine-concept present in engine we are deleting
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite to all its unique engine-concept-property composite
//				+ "{?conceptComposite ?prop ?propertyComposite}"
////				+ "FILTER("
////				// filters for relationships
////				+ 	" ?prop = <http://www.w3.org/2002/07/owl#DatatypeProperty> "
////				+ 	")"
//				+ "}";
//		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//
//		// delete unique engine-concept-property composite to its physical URI
//		query = "SELECT ?propertyComposite ?propertyType ?propertyPhysical WHERE {"
//				+ "BIND( <http://www.w3.org/2002/07/owl#DatatypeProperty> AS ?prop) "
//				+ "BIND( <" + RDF.TYPE + "> AS ?propertyType ) "
//				// unique engine-concept present in engine we are deleting
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite to all its unique engine-concept-property composite
//				+ "{?conceptComposite ?prop ?propertyComposite}"
//				// unique engine-concept-property composite is a type of a physical uri
//				+ "{?propertyComposite ?propertyType ?propertyPhysical}"
////				+ "FILTER("
////				// filters for relationships
////				+ 	" ?prop = <http://www.w3.org/2002/07/owl#DatatypeProperty> && "
////				+ 	" ?propertyType = <" + RDF.TYPE + "> "
////				+ 	")"
//				+ "}";
//		manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//
//		// // delete all logical names assigned to this engine-concept-property composite
//		query = "SELECT DISTINCT ?propertyComposite ?propertyLogicalRel ?propertyLogical WHERE {"
//				+ "BIND( <http://www.w3.org/2002/07/owl#DatatypeProperty> AS ?prop) "
//				+ "BIND( <http://semoss.org/ontologies/Relation/logical> AS ?propertyLogicalRel ) "
//				// unique engine-concept present in engine we are deleting
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite to all its unique engine-concept-property composite
//				+ "{?conceptComposite ?prop ?propertyComposite}"
//				// unique engine-concept-property composite its its logical names
//				+ "{?propertyComposite ?propertyLogicalRel ?propertyLogical}"
////				+ "FILTER("
////				// filters for relationships
////				+ 	" ?prop = <http://www.w3.org/2002/07/owl#DatatypeProperty> &&  "
////				+ 	" ?propertyLogicalRel = <http://semoss.org/ontologies/Relation/logical> "
////				+ 	")"
//				+ "}";
//		manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//
//		// delete the conceptual name for the engine-concept-property composite
//		query = "SELECT DISTINCT ?propertyComposite ?propertyConceptualRel ?propertyConceptual WHERE {"
//				+ "BIND( <http://www.w3.org/2002/07/owl#DatatypeProperty> AS ?prop) "
//				+ "BIND( <http://semoss.org/ontologies/Relation/conceptual> AS ?propertyConceptualRel ) "
//				// unique engine-concept present in engine we are deleting
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite to all its unique engine-concept-property composite
//				+ "{?conceptComposite ?prop ?propertyComposite}"
//				// unique engine-concept composite to its conceptual name
//				+ "{?propertyComposite ?propertyConceptualRel ?propertyConceptual}"
////				+ "FILTER("
////				// filters for relationships
////				+ 	" ?prop = <http://www.w3.org/2002/07/owl#DatatypeProperty> && "
////				+ 	" ?propertyConceptualRel = <http://semoss.org/ontologies/Relation/conceptual>"
////				+ 	")"
//				+ "}";
//		manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//	}
//
//	/**
//	 * Remove engine relationships
//	 * @param engineName
//	 * @param localMaster
//	 * @param deleteTriplesList
//	 */
//	private void addEngineRelationshipTriplesToDelete(String engineName, IEngine localMaster, List<Object[]> deleteTriplesList) {
//		String whereStatement = " WHERE { "
//				// make sure node is in the engine
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// make sure other node is in the engine
//				+ "{?anotherConceptConcept <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// relationship between the two nodes
//				+ "{?conceptComposite ?rel ?anotherConceptConcept}"
//				// any inferencing crap that is associated with the relationship between the nodes
//				+ "{?rel ?anyPred ?anyObj}"
//				+ "}";
//
//		String query = "SELECT DISTINCT ?conceptComposite ?rel ?anotherConceptConcept " + whereStatement;
//		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//
//		query = "SELECT DISTINCT ?rel ?anyPred ?anyObj " + whereStatement;
//		manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//	}
//
//	/**
//	 * Wipe out anything left with the engine
//	 * @param engineName
//	 * @param fromEngine
//	 */
//	private void addEngineMetadataToDelete(String engineName, IEngine localMaster, List<Object[]> deleteTriplesList) {
//		String query = "SELECT DISTINCT ?engine ?anyPred ?anyObj WHERE {"
//				+ "BIND(<http://semoss.org/ontologies/meta/engine/" + engineName + "> AS ?engine) "
//				+ "{?engine ?anyPred ?anyObj}"
////				+ "FILTER(?engine = <http://semoss.org/ontologies/meta/engine/" + engineName + ">)"
//				+ "}";
//		IRawSelectWrapper manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//
//		query = "SELECT DISTINCT ?anysub ?anyPred ?engine WHERE {"
//				+ "BIND(<http://semoss.org/ontologies/meta/engine/" + engineName + "> AS ?engine) "
//				+ "{?anysub ?anyPred ?engine}"
////				+ "FILTER(?engine = <http://semoss.org/ontologies/meta/engine/" + engineName + ">)"
//				+ "}";
//		manager = WrapperManager.getInstance().getRawWrapper(localMaster, query);
//		while(manager.hasNext()) {
//			IHeadersDataRow stmt = manager.next();
//			// make a new array to hold the object
//			Object[] data = new Object[4];
//			// copy the triple from the query into the data array
//			System.arraycopy(stmt.getRawValues(), 0, data, 0, 3);
//			// set the last boolean so the remove method knows this is a URI vs. Literal
//			data[3] = true;
////			System.out.println(Arrays.toString(data));
//			deleteTriplesList.add(data);
//		}
//	}
//
//	/**
//	 * Deletes all triples in the master database.
//	 * @throws SailException
//	 */
//	public void deleteAll() throws SailException{
//		masterEngine.removeData("DELETE {?x ?y ?z}");
//	}
//
//	
//	///////////////////////////////////////////////////////////////////////////////
//	///////////////////////////////////////////////////////////////////////////////
//	////////////////////////// using delete query logic ///////////////////////////
//	///////////////////////////////////////////////////////////////////////////////
//	///////////////////////////////////////////////////////////////////////////////
//	
//	/**
//	 * Delete concepts with properties triples
//	 * @param engineName
//	 * @param fromEngine
//	 */
//	private void deleteEngineConceptsWithProperties(String engineName, IEngine fromEngine)
//	{
//		// first get rid of concept with properties
//		// need to delete logical
//		// need to delete conceptual
//		String deleteQuery = "DELETE "
//				// TRIPLES TO DELETE
//				+ "{"
//				// delete unique engine-concept composite subclass of stuff
//				+ "?conceptComposite ?subclass ?rdfConcept . "
//				// delete unique engine-concept composite to its physical URI
//				+ "?conceptComposite ?conceptType ?conceptPhysical . "
//				// delete all logical names assigned to this engine-concept composite
//				+ "?conceptComposite ?conceptLogicalRel ?conceptLogical . "
//				// delete the conceptual name for the engine-concept composite
//				+ "?conceptComposite ?conceptConceptualRel ?conceptualName . "
//				// delete the relationship between the engine-concept composite to engine-concept-property composite
//				+ "?conceptComposite ?prop ?propertyComposite . "
//				// delete unique engine-concept-property composite to its physical URI
//				+ "?propertyComposite ?propertyType ?propertyPhysical . "
//				// delete all logical names assigned to this engine-concept-property composite
//				+ "?propertyComposite ?propertyLogicalRel ?propertyLogical . "
//				// delete the conceptual name for the engine-concept composite
//				+ "?propertyComposite ?propertyConceptualRel ?propertyConceptual . "
//
//				// PATTERN TO MATCH
//				+ "} WHERE { "
//				// SECTION TO GET CONCEPT TRIPLES
//				// unique engine-concept composite subclass of stuff
//				+ "{?conceptComposite ?subclass ?rdfConcept}"
//				// unique engine-concept composite is a type of a physical uri
//				+ "{?conceptComposite ?conceptType ?conceptPhysical}"
//				// unique engine-concept present in engine we are deleting
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite its its logical names
//				+ "{?conceptComposite ?conceptLogicalRel ?conceptLogical}"
//				// unique engine-concept composite to its conceptual name
//				+ "{?conceptComposite ?conceptConceptualRel ?conceptualName}"
//				// unique engine-concept composite to all its unique engine-concept-property composite
//				+ "{?conceptComposite ?prop ?propertyComposite}"
//
//				// SECTION TO GET PROPERTY TRIPLES
//				// unique engine-concept-property composite is a type of a physical uri
//				+ "{?propertyComposite ?propertyType ?propertyPhysical}"
//				// unique engine-concept-property composite its its logical names
//				+ "{?propertyComposite ?propertyLogicalRel ?propertyLogical}"
//				// unique engine-concept-property composite to its conceptual name
//				+ "{?propertyComposite ?propertyConceptualRel ?propertyConceptual}"
//
//				// SECTION TO MAKE SURE RELATIONSHIPS ARE WHAT WE WANT
//				+ "FILTER("
//				// filters for concept relationships
//				+ 	" ?subclass = <" + RDFS.subClassOf + "> && "
//				+ 	" ?conceptType = <" + RDF.TYPE + "> && "
//				+ 	" ?conceptLogicalRel = <http://semoss.org/ontologies/Relation/logical> && "
//				+ 	" ?conceptConceptualRel = <http://semoss.org/ontologies/Relation/conceptual> && "
//				+ 	" ?prop = <http://www.w3.org/2002/07/owl#DatatypeProperty> && "
//				// filters for concept properties
//				+ 	" ?propertyType = <" + RDF.TYPE + "> && "
//				+ 	" ?propertyLogicalRel = <http://semoss.org/ontologies/Relation/logical> && "
//				+ 	" ?propertyConceptualRel = <http://semoss.org/ontologies/Relation/conceptual>"
//				+ 	")"
//				+ "}";
//
//		long startTime = System.currentTimeMillis();
//
//		fromEngine.insertData(deleteQuery);	
//
//		long endTime = System.currentTimeMillis();
//
//		LOGGER.info("TIME TO DELETE CONCEPTS W/ PROPERTIES ::: " + (endTime - startTime) + " ms" );
//	}
//
//	/**
//	 * Delete concepts without properties
//	 * @param engineName
//	 * @param fromEngine
//	 */
//	private void deleteEngineConceptsWithoutProperties(String engineName, IEngine fromEngine)
//	{
//		// delete concepts without properties
//		// need to delete logical
//		String deleteQuery = "DELETE "
//				// TRIPLES TO DELETE
//				+ "{"
//				// delete unique engine-concept composite subclass of stuff
//				+ "?conceptComposite ?subclass ?rdfConcept . "
//				// delete unique engine-concept composite to its physical URI
//				+ "?conceptComposite ?conceptType ?conceptPhysical . "
//				// delete all logical names assigned to this engine-concept composite
//				+ "?conceptComposite ?conceptLogicalRel ?conceptLogical . "
//				// delete the conceptual name for the engine-concept composite
//				+ "?conceptComposite ?conceptConceptualRel ?conceptualName . "               
//
//				// PATTERN TO MATCH
//				+ "} WHERE { "
//				// SECTION TO GET CONCEPT TRIPLES
//				// unique engine-concept composite subclass of stuff
//				+ "{?conceptComposite ?subclass ?rdfConcept}"
//				// unique engine-concept composite is a type of a physical uri
//				+ "{?conceptComposite ?conceptType ?conceptPhysical}"
//				// unique engine-concept present in engine we are deleting
//				+ "{?conceptComposite <http://semoss.org/ontologies/Relation/presentin> <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// unique engine-concept composite its its logical names
//				+ "{?conceptComposite ?conceptLogicalRel ?conceptLogical}"
//				// unique engine-concept composite to its conceptual name
//				+ "{?conceptComposite ?conceptConceptualRel ?conceptualName}"
//
//				// SECTION TO MAKE SURE RELATIONSHIPS ARE WHAT WE WANT
//				+ "FILTER("
//				// filters for concept relationships
//				+ 	" ?subclass = <" + RDFS.subClassOf + "> && "
//				+ 	" ?conceptType = <" + RDF.TYPE + "> && "
//				+ 	" ?conceptLogicalRel = <http://semoss.org/ontologies/Relation/logical> && "
//				+ 	" ?conceptConceptualRel = <http://semoss.org/ontologies/Relation/conceptual> && "
//				+ 	" ?prop = <http://www.w3.org/2002/07/owl#DatatypeProperty>"
//				+ 	")"
//				+"}";
//
//		long startTime = System.currentTimeMillis();
//
//		fromEngine.insertData(deleteQuery);	
//
//		long endTime = System.currentTimeMillis();
//
//		LOGGER.info("TIME TO DELETE CONCEPTS W/O PROPERTIES ::: " + (endTime - startTime) + " ms" );
//	}
//
//	/**
//	 * Delete relationships
//	 * @param engineName
//	 * @param fromEngine
//	 */
//	private void deleteRelations(String engineName, IEngine fromEngine)
//	{
//		String deleteQuery = "DELETE "
//				+ "{"
//				// delete any triple between two unique engine-concept composite nodes
//				+ "?conceptComposite ?rel ?anotherConceptConcept . "
//				// delete any triples associated with the predicate above
//				+ "?rel ?anyPred ?anyObj . "
//				+ "} WHERE { "
//				// make sure node is in the engine
//				+ "{?conceptComposite ?in  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// make sure other node is in the engine
//				+ "{?anotherConceptConcept ?in  <http://semoss.org/ontologies/meta/engine/" + engineName + ">}"
//				// relationship between the two nodes
//				+ "{?concept ?rel ?anotherConcept}"
//				// any inferencing crap that is associated with the relationship between the nodes
//				+ "{?rel ?anyPred ?anyObj}"
//				+ "}";
//
//		long startTime = System.currentTimeMillis();
//
//		fromEngine.insertData(deleteQuery);	
//
//		long endTime = System.currentTimeMillis();
//
//		LOGGER.info("TIME TO DELETE RELATIONSHIPS ::: " + (endTime - startTime) + " ms" );
//	}
//	
//	// private
//
//	/**
//	 * Wipe out anything left with the engine
//	 * @param engineName
//	 * @param fromEngine
//	 */
//	private void deleteEngineMetadata(String engineName, IEngine fromEngine)
//	{
//		String deleteQuery = "DELETE {"
//				+ "?engine ?anyPred ?anyObj."
//				+ "?anysub ?anyPred2 ?engine."
//				+ "} WHERE {"
//				+ "{?engine ?anyPred ?anyObj}"
//				+ "{?anysub ?anyPred2 ?engine}"
//				+ "FILTER(?engine = <http://semoss.org/ontologies/meta/engine/" + engineName + ">)"
//				+ "}";
//
//		long startTime = System.currentTimeMillis();
//
//		fromEngine.insertData(deleteQuery);	
//
//		long endTime = System.currentTimeMillis();
//
//		LOGGER.info("TIME TO DELETE ENGINE NODE ::: " + (endTime - startTime) + " ms" );
//	}
//
}
