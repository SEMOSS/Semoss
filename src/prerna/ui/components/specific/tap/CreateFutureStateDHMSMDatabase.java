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
package prerna.ui.components.specific.tap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractDatabaseEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.Utility;

public class CreateFutureStateDHMSMDatabase extends AggregationHelper {

	private final String CURR_ICD_AND_WEIGHT_QUERY = "SELECT DISTINCT ?icd ?weight WHERE{ {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemInterface>} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>} {?icd ?payload ?data} {?payload <http://semoss.org/ontologies/Relation/Contains/TypeWeight> ?weight} }";
	private final String SYSTEM_QUERY = "SELECT DISTINCT ?System WHERE{ {?System a <http://semoss.org/ontologies/Concept/System>}}";

	private final String NEW_ICD_TYPE = "http://semoss.org/ontologies/Concept/ProposedSystemInterface";
	private final String REMOVED_ICD_TYPE = "http://semoss.org/ontologies/Concept/ProposedDecommissionedSystemInterface";
	private final String ICD_TYPE = "http://semoss.org/ontologies/Concept/SystemInterface";

	private IDatabaseEngine tapCore;
	private IDatabaseEngine futureState;
	private IDatabaseEngine futureCostState;

	private List<Object[]> relList;
	private List<Object[]> relPropList;
	private List<String> addedInterfaces;
	private List<String> removedInterfaces;
	private Set<String> sysList;
	private List<Object[]> trainingGLPropList;
	
	private List<Object[]> relCostList;
	private List<Object[]> loeList;
	private Set<String> sysCostList;
	private Set<String> glItemList;
	private Set<String> labelList;
	private Set<String> labelCostList;
	private Set<String> getSysTrainingList;

	private HashMap<String, HashMap<String, Set<String>>> baseFutureRelations;
	private HashMap<String, HashMap<String, Set<String>>> baseFutureCostRelations;

	public CreateFutureStateDHMSMDatabase() {

	}

	public CreateFutureStateDHMSMDatabase(IDatabaseEngine tapCore, IDatabaseEngine futureState, IDatabaseEngine futureCostState) {
		this.tapCore = tapCore;
		this.futureState = futureState;
		this.futureCostState = futureCostState;
	}

	public void setTapCore(IDatabaseEngine tapCore) {
		this.tapCore = tapCore;
	}

	public void setFutureState(IDatabaseEngine futureState) {
		this.futureState = futureState;
	}

	public void setFutureCostState(IDatabaseEngine futureCostState) {
		this.futureCostState = futureCostState;
	}

	public void createDBs() throws RepositoryException, RDFHandlerException, IOException {
		createFutureStateDB();
		createFutureStateCostDB();
	}

	public void createFutureStateCostDB() throws IOException, RepositoryException, RDFHandlerException {
		if (relCostList == null || loeList == null || sysCostList == null || glItemList == null) {
			generateData();
		}
		dataHash.clear();
		allConcepts.clear();
		allRelations.clear();
		allLabels.clear();

		Set<String> currSys = addActiveSystems();
		sysCostList.addAll(currSys);
		getSysTrainingList.addAll(currSys);
		
		baseFutureCostRelations = new HashMap<String, HashMap<String, Set<String>>>();
		generateTrainingGLItems();
		processInstanceDataRelations(relCostList, baseFutureCostRelations);
		processInstancePropOnNodeData(loeList, futureCostState);
		processInstancePropOnNodeData(trainingGLPropList, futureCostState);
		processData(futureCostState, dataHash);
		// process the high lvl node data
		processAllConceptTypeTriples(futureCostState);
		// process the high lvl rel data
		processAllRelationshipSubpropTriples(futureCostState);

		// add subclassing for systems
		processActiveSystemSubclassing(futureCostState, sysCostList);
		// add subclassing for glitems
		processGlItemsSubclassing(futureCostState, glItemList);

		for (String instance : labelCostList) {
			addToAllLabel(instance);
		}
		processLabel(futureCostState);

		((BigDataEngine) futureCostState).commit();
		((BigDataEngine) futureCostState).infer();
		writeToOWL(futureCostState, baseFutureCostRelations);
		// update base filter hash
		((AbstractDatabaseEngine) futureCostState).createBaseRelationEngine();
	}

	public void createFutureStateDB() throws IOException, RepositoryException, RDFHandlerException {
		if (relList == null || relPropList == null || addedInterfaces == null || removedInterfaces == null) {
			generateData();
		}
		dataHash.clear();
		allConcepts.clear();
		allRelations.clear();
		allLabels.clear();

		baseFutureRelations = new HashMap<String, HashMap<String, Set<String>>>();
		processInstanceDataRelations(relList, baseFutureRelations);
		processInstancePropOnRelationshipData(relPropList, futureState);

		processData(futureState, dataHash);
		// process the high lvl node data
		processAllConceptTypeTriples(futureState);
		// process the high lvl rel data
		processAllRelationshipSubpropTriples(futureState);

		// add sub-classing for systems
		processActiveSystemSubclassing(futureState, sysList);

		// add sub-classing of icd's
		processNewSubclass(futureState, ICD_TYPE, NEW_ICD_TYPE);
		processNewSubclass(futureState, ICD_TYPE, REMOVED_ICD_TYPE);
		for (String addedICD : addedInterfaces) {
			processNewConceptsAtInstanceLevel(futureState, addedICD, NEW_ICD_TYPE);
		}
		for (String removedICD : removedInterfaces) {
			processNewConceptsAtInstanceLevel(futureState, removedICD, REMOVED_ICD_TYPE);
		}

		for (String instance : labelList) {
			addToAllLabel(instance);
		}
		processLabel(futureState);

		((BigDataEngine) futureState).commit();
		((BigDataEngine) futureState).infer();
		writeToOWL(futureState, baseFutureRelations);
		// update base filter hash
		((AbstractDatabaseEngine) futureState).createBaseRelationEngine();
	}

	public void processGlItemsSubclassing(IDatabaseEngine engine, Set<String> data) {
		processNewConcepts(engine, "http://semoss.org/ontologies/Concept/GLItem");
		processNewConcepts(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/GLItem", "http://semoss.org/ontologies/Concept/TransitionGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/RequirementsGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/DesignGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/DevelopGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/TestGLItem");
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/TransitionGLItem", "http://semoss.org/ontologies/Concept/DeployGLItem");
		for (String glItemURI : data) {
			processNewConceptsAtInstanceLevel(engine, glItemURI, "http://semoss.org/ontologies/Concept/TransitionGLItem");
		}
	}

	public void generateData() throws IOException {
		LPInterfaceDBModProcessor processor = new LPInterfaceDBModProcessor();
		processor.setEngine(tapCore);

		Map<String, Map<String, String>> providerFutureIcdFrequency = DHMSMTransitionUtility.getProviderFutureICDFrequency(tapCore);
		Map<String, Map<String, String>> consumerFutureIcdFrequency = DHMSMTransitionUtility.getConsumerFutureICDFrequency(tapCore);
		processor.setProviderFutureICDProp(providerFutureIcdFrequency);
		processor.setConsumerFutureICDProp(consumerFutureIcdFrequency);
		
		processor.generateTriples();
		relList = processor.getRelList();
		relPropList = processor.getRelPropList();
		addedInterfaces = processor.getAddedInterfaces();
		removedInterfaces = processor.getRemovedInterfaces();
		sysList = processor.getSysList();

		relCostList = processor.getCostRelList();
		loeList = processor.getLoeList();
		sysCostList = processor.getSysCostList();
		glItemList = processor.getGlItemList();
		getSysTrainingList = processor.getSysTrainingList();
		
		labelList = processor.getLabelList();
		labelCostList = processor.getLabelCostList();
	}

	public void addTriplesToExistingICDs() {
		ISelectWrapper sjsw = Utility.processQuery(futureState, CURR_ICD_AND_WEIGHT_QUERY);
		String[] varNames = sjsw.getVariables();
		while (sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String icdURI = sjss.getRawVar(varNames[0]).toString();
			Double weight = (Double) sjss.getVar(varNames[1]);
			if (weight.doubleValue() == 5) {
				processNewConceptsAtInstanceLevel(futureState, icdURI, NEW_ICD_TYPE);
			} else if (weight.doubleValue() == 0) {
				processNewConceptsAtInstanceLevel(futureState, icdURI, REMOVED_ICD_TYPE);
			}
		}
	}

	// add all systems to active system
	private Set<String> addActiveSystems() {
		ISelectWrapper wrapper = Utility.processQuery(futureCostState, SYSTEM_QUERY);
		String[] names = wrapper.getVariables();
		Set<String> retSet = new HashSet<String>();
		while (wrapper.hasNext()) {
			retSet.add(wrapper.next().getRawVar(names[0]).toString());
		}
		return retSet;
	}


	public void generateTrainingGLItems() {
		String trainingGLItemURI = "http://semoss.org/ontologies/Concept/TrainingGLItem";
		String instanceGLItemURIBase = "http://health.mil/ontologies/Concept/TrainingGLItem";

		// create GLtag and say its a type of concept
		String trainingGLTag = "http://health.mil/ontologies/Concept/GLTag/Training"; //"use health.mil";
		String baseTrainingGLTag = "http://semoss.org/ontologies/Concept/GLTag";  //"use semoss.org";

		// only adding GLTag of type training
		Set<String> trainingGLTagSet = new HashSet<String>();
		trainingGLTagSet.add(trainingGLTag);
		allConcepts.put(baseTrainingGLTag, trainingGLTagSet);
		addToAllLabel(trainingGLTag);

		Set<String> instanceTrainingGLITems = new HashSet<String>();
		Set<String> instanceInfluenceRelationships = new HashSet<String>();
		Set<String> instanceTaggedByRelationships = new HashSet<String>();

		trainingGLPropList = new ArrayList<Object[]>();
		for (String system : getSysTrainingList) {
			String sysName = Utility.getInstanceName(system);

			String trainingGLItemName = "Training%Training%" + sysName + "%Training";
			String trainingGLItemInstanceURI = instanceGLItemURIBase + "/" + trainingGLItemName;
			instanceTrainingGLITems.add(trainingGLItemInstanceURI);

			String influencePredicateString =  "http://health.mil/ontologies/Relation/Influences/" + sysName + ":" + trainingGLItemName;
			instanceInfluenceRelationships.add(influencePredicateString);

			String taggedPredicateString =  "http://health.mil/ontologies/Relation/TaggedBy/" + sysName + ":" + trainingGLItemName;
			instanceTaggedByRelationships.add(taggedPredicateString);

			relCostList.add(new Object[]{system, influencePredicateString, trainingGLItemInstanceURI});
			relCostList.add(new Object[]{trainingGLItemInstanceURI, taggedPredicateString, trainingGLTag});
			trainingGLPropList.add(new Object[]{trainingGLItemInstanceURI, "http://semoss.org/ontologies/Relation/Contains/Rate", 0.15});
			
			addToAllLabel(trainingGLItemInstanceURI);
			addToAllLabel(influencePredicateString);
			addToAllLabel(taggedPredicateString);
		}
		allConcepts.put(trainingGLItemURI, instanceTrainingGLITems);
		if(allRelations.containsKey("http://semoss.org/ontologies/Relation/Influences")) {
			allRelations.get("http://semoss.org/ontologies/Relation/Influences").addAll(instanceInfluenceRelationships);
		} else {
			allRelations.put("http://semoss.org/ontologies/Relation/Influences", instanceInfluenceRelationships);
		}

		if(allRelations.containsKey("http://semoss.org/ontologies/Relation/TaggedBy")) {
			allRelations.get("http://semoss.org/ontologies/Relation/TaggedBy").addAll(instanceInfluenceRelationships);
		} else {
			allRelations.put("http://semoss.org/ontologies/Relation/TaggedBy", instanceInfluenceRelationships);
		}
	}


}
