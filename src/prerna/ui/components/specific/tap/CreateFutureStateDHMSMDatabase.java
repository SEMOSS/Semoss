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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.util.Utility;

public class CreateFutureStateDHMSMDatabase extends AggregationHelper {

	private final String CURR_ICD_AND_WEIGHT_QUERY = "SELECT DISTINCT ?icd ?weight WHERE{ {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?icd ?payload ?data} {?payload <http://semoss.org/ontologies/Relation/Contains/TypeWeight> ?weight} }";
	
	private final String NEW_ICD_TYPE = "http://semoss.org/ontologies/Concept/ProposedInterfaceControlDocument";
	private final String REMOVED_ICD_TYPE = "http://semoss.org/ontologies/Concept/ProposedDecommissionedInterfaceControlDocument";
	private final String ICD_TYPE = "http://semoss.org/ontologies/Concept/InterfaceControlDocument";
	
	private IEngine hrCore;
	private IEngine futureState;
	private IEngine futureCostState;
	private IEngine tapCost;
	
	private ArrayList<Object[]> relList;
	private ArrayList<Object[]> relPropList;
	private ArrayList<String> addedInterfaces;
	private ArrayList<String> removedInterfaces;
	private Set<String> sysList;
	
	private ArrayList<Object[]> relCostList;
	private ArrayList<Object[]> loeList;
	private Set<String> sysCostList;
	private Set<String> glItemList;
	
	private HashMap<String, HashMap<String, Set<String>>> baseFutureRelations;
	private HashMap<String, HashMap<String, Set<String>>> baseFutureCostRelations;

	public CreateFutureStateDHMSMDatabase() {
		
	}
	
	public CreateFutureStateDHMSMDatabase(IEngine hrCore, IEngine futureState, IEngine futureCostDB, IEngine tapCost) {
		this.hrCore = hrCore;
		this.futureState = futureState;
		this.futureCostState = futureCostDB;
		this.tapCost = tapCost;
	}
	
	public void setHrCore(IEngine hrCore) {
		this.hrCore = hrCore;
	}
	
	public void setFutureState(IEngine futureState) {
		this.futureState = futureState;
	}
	
	public void setFutureCostState(IEngine futureCostState) {
		this.futureCostState = futureCostState;
	}
	
	public void createDBs() throws RepositoryException, RDFHandlerException, EngineException{
		createFutureStateDB();
		createFutureStateCostDB();
	}
	
	public void createFutureStateCostDB() throws EngineException, RepositoryException, RDFHandlerException {
		if(relCostList == null || loeList == null || sysCostList == null || glItemList == null) {
			generateData();
		}
		dataHash.clear();
		allConcepts.clear();
		allRelations.clear();
		
		baseFutureCostRelations = new HashMap<String, HashMap<String, Set<String>>>();
		processInstanceDataRelations(relCostList, baseFutureCostRelations);
		processInstancePropOnNodeData(loeList, futureCostState);
		processData(futureCostState, dataHash);
		// process the high lvl node data
		processAllConceptTypeTriples(futureCostState);
		// process the high lvl rel data
		processAllRelationshipSubpropTriples(futureCostState);
		
		// add subclassing for systems
		processActiveSystemSubclassing(futureCostState, sysCostList);
		// add subclassing for glitems
		processGlItemsSubclassing(futureCostState, glItemList);
				
		((BigDataEngine) futureCostState).commit();
		((BigDataEngine) futureCostState).infer();
		writeToOWL(futureCostState, baseFutureCostRelations);
		// update base filter hash
		((AbstractEngine) futureCostState).createBaseRelationEngine();
	}
	
	public void createFutureStateDB() throws EngineException, RepositoryException, RDFHandlerException {
		if(relList == null || relPropList == null || addedInterfaces == null || removedInterfaces == null) {
			generateData();
		}
		dataHash.clear();
		allConcepts.clear();
		allRelations.clear();
		
		baseFutureRelations = new HashMap<String, HashMap<String, Set<String>>>();
		processInstanceDataRelations(relList, baseFutureRelations);
		processInstancePropOnRelationshipData(relPropList, futureState);
		processData(futureState, dataHash);
		// process the high lvl node data
		processAllConceptTypeTriples(futureState);
		// process the high lvl rel data
		processAllRelationshipSubpropTriples(futureState);
		
		//add sub-classing for systems
		processActiveSystemSubclassing(futureState, sysList);
		
		// add sub-classing of icd's
		processNewSubclass(futureState, ICD_TYPE, NEW_ICD_TYPE);
		processNewSubclass(futureState, ICD_TYPE, REMOVED_ICD_TYPE);
		for(String addedICD: addedInterfaces) {
			processNewConceptsAtInstanceLevel(futureState, addedICD, NEW_ICD_TYPE);
		}
		for(String removedICD: removedInterfaces) {
			processNewConceptsAtInstanceLevel(futureState, removedICD, REMOVED_ICD_TYPE);
		}
		
		((BigDataEngine) futureState).commit();
		((BigDataEngine) futureState).infer();
		writeToOWL(futureState, baseFutureRelations);
		// update base filter hash
		((AbstractEngine) futureState).createBaseRelationEngine();
	}
	
	public void processGlItemsSubclassing(IEngine engine, Set<String> data){
		processNewConcepts(engine, "http://semoss.org/ontologies/Concept/GLItem");
		processNewConcepts(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/GLItem", "http://semoss.org/ontologies/Concept/TransitionGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/RequirementsGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/DesignGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/DevelopGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/TestGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/DeployGLItem");
		for(String glItemURI : data) {
			processNewConceptsAtInstanceLevel(engine, glItemURI, "http://semoss.org/ontologies/Concept/TransitionGLItem");
		}
	}
	
	public void generateData() throws EngineException {
		LPInterfaceProcessor processor = new LPInterfaceProcessor();
		processor.setEngine(hrCore);
//		processor.isGenerateCost(true);
//		processor.setUsePhase(true);
		processor.setGenerateNewTriples(true);
		
		processor.getCostInfoAtPhaseLevel(tapCost);
		processor.generateReport();
		relList = processor.getRelList();
		relPropList = processor.getPropList();
		addedInterfaces = processor.getAddedInterfaces();
		removedInterfaces = processor.getRemovedInterfaces();
		sysList = processor.getSysList();
		
		relCostList = processor.getCostRelList();
		loeList = processor.getLoeList();
		sysCostList = processor.getSysCostList();
		glItemList = processor.getGlItemList();
	}
	
	public void addTriplesToExistingICDs(){
		ISelectWrapper sjsw = Utility.processQuery(futureState, CURR_ICD_AND_WEIGHT_QUERY);
		String[] varNames = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String icdURI = sjss.getRawVar(varNames[0]).toString();
			Double weight = (Double) sjss.getVar(varNames[1]);
			if(weight.doubleValue() == 5) {
				processNewConceptsAtInstanceLevel(futureState, icdURI, NEW_ICD_TYPE);
			} else if(weight.doubleValue() == 0){
				processNewConceptsAtInstanceLevel(futureState, icdURI, REMOVED_ICD_TYPE);
			}
		}
	}
	
}
