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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.util.Utility;

/**
 * This class is used to generate items in the GL and is used to update the cost database.
 */
public class GLItemGeneratorSelfReportedFutureInterfaces extends AggregationHelper{

	private IEngine hrCore;
	private IEngine futureDB;
	private IEngine futureCostDB;
	private String genSpecificDProtQuery = "SELECT DISTINCT ?icd ?data ?sys (COALESCE(?dprot, (URI(\"http://health.mil/ontologies/Concept/DProt/HTTPS-SOAP\"))) AS ?Prot) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} OPTIONAL{ ?payload <http://semoss.org/ontologies/Relation/Contains/Protocol> ?dprot} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String genSpecificDFormQuery = "SELECT DISTINCT ?icd ?data ?sys (COALESCE(?dform, (URI(\"http://health.mil/ontologies/Concept/DForm/XML\"))) AS ?Form) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} OPTIONAL{ ?payload <http://semoss.org/ontologies/Relation/Contains/Format> ?dform} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String providerDataQuery1 = "SELECT DISTINCT ?icd ?data ?sys WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?upstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?sys ?upstream ?icd ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;}  FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String providerDataQuery2 = "";
	private String consumerDataQuery = "SELECT DISTINCT ?icd ?data ?sys WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	
	private final String insertLOEQuery = "SELECT DISTINCT ?GLitem ?pred ?combinedTOTAL WHERE { BIND(<http://semoss.org/ontologies/Relation/Contains/LOEcalc> AS ?pred) { SELECT DISTINCT ?GLitem (ROUND(SUM((?multipliedTotal*(1+?Rate)))) AS ?combinedTOTAL) WHERE { {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> .} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass ;} BIND(<http://semoss.org/ontologies/Relation/Includes> AS ?contains) {?glItemCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLItemCoreTask> ;} {?GLitem ?contains ?glItemCoreTask .} {SELECT DISTINCT ?glItemCoreTask (SUM(?factor*?LOE) AS ?multipliedTotal) WHERE { BIND(<http://semoss.org/ontologies/Relation/Includes> AS ?contains2) {?GLItemSubTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLItemSubTask> ;} {?glItemCoreTask ?contains2 ?GLItemSubTask ;} BIND(<http://semoss.org/ontologies/Relation/Estimated> AS ?type) {?TargetPhaseBasisSubTaskComplexityComplexity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TargetPhaseBasisSubTaskComplexityComplexity> ;} {?GLItemSubTask ?type ?TargetPhaseBasisSubTaskComplexityComplexity ;} {?GLItemSubTask <http://semoss.org/ontologies/Relation/Contains/Factor> ?factor ;}{?TargetPhaseBasisSubTaskComplexityComplexity <http://semoss.org/ontologies/Relation/Contains/LOE> ?LOE ;}} GROUP BY ?glItemCoreTask } {SELECT DISTINCT ?glItemCoreTask (SUM(?rate) AS ?Rate) WHERE { BIND(<http://semoss.org/ontologies/Relation/TypeOf> AS ?TypeOf) {?TargetPhaseBasisCoreTask <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TargetPhaseBasisCoreTask> ;} {?glItemCoreTask ?TypeOf ?TargetPhaseBasisCoreTask ;} BIND(<http://semoss.org/ontologies/Relation/Incurs> AS ?incurs) {?TargetOverheadItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TargetOverheadItem> ;} {?TargetPhaseBasisCoreTask ?incurs ?TargetOverheadItem ;} {?TargetOverheadItem <http://semoss.org/ontologies/Relation/Contains/Rate> ?rate ;} } GROUP BY ?glItemCoreTask} }GROUP BY ?GLitem } }";
	
	private HashMap<String, HashMap<String, Set<String>>> baseFutureCostRelations = new HashMap<String, HashMap<String, Set<String>>>();
	
	/**
	 * Constructor for GLItemGeneratorSelfReportedFutureInterfaces.
	 */
	public GLItemGeneratorSelfReportedFutureInterfaces(IEngine hrCore, IEngine futureState, IEngine futureCostDB) {
		this.hrCore = hrCore;
		this.futureDB = futureState;
		this.futureCostDB = futureCostDB;
	}

	public void genData() throws RepositoryException, RDFHandlerException{
		GLItemGeneratorICDValidated generator = new GLItemGeneratorICDValidated();
		prepareGLItemGenerator(generator);
		runGenerator(generator);
		Hashtable<String, Vector<String[]>> allData = generator.getAllDataHash();
		getData(allData);
		
		addGLItemSubclassing();
		
		addLOEcalc();
		
		((BigDataEngine) futureCostDB).commit();
		((BigDataEngine) futureCostDB).infer();
		writeToOWL(futureCostDB, baseFutureCostRelations);
	}
	
	public void getData(Hashtable<String, Vector<String[]>> allData) {
		//list of nodes
		final String SYSTEM = "http://health.mil/ontologies/Concept/System/";
		final String DATA_OBJECT = "http://health.mil/ontologies/Concept/DataObject/";
		final String ICD = "http://health.mil/ontologies/Concept/InterfaceControlDocument/";
		final String DESIGN_GLITEM = "http://health.mil/ontologies/Concept/DesignGLItem/";
		final String TEST_GLITEM = "http://health.mil/ontologies/Concept/TestGLItem/";
		final String DEVELOP_GLITEM = "http://health.mil/ontologies/Concept/DevelopGLItem/";
		final String REQUIREMENTS_GLITEM = "http://health.mil/ontologies/Concept/RequirementsGLItem/";
		final String GLITEM_CORE_TASK = "http://health.mil/ontologies/Concept/GLItemCoreTask/";
		final String GLITEM_SUB_TASK = "http://health.mil/ontologies/Concept/GLItemSubTask/";
		final String TARGET_PHASE_BASIS_CORE_TASK = "http://health.mil/ontologies/Concept/TargetPhaseBasisCoreTask/";
		final String TARGET_PHASE_BASIS_SUB_TASK_COMPLEXITY_COMPLEXITY = "http://health.mil/ontologies/Concept/TargetPhaseBasisSubTaskComplexityComplexity/";
		final String PHASE = "http://health.mil/ontologies/Concept/SDLCPhase/";
		final String GLTAG = "http://health.mil/ontologies/Concept/GLTag/";
	
		//list of relationships
		final String INPUT = "http://health.mil/ontologies/Relation/Input/";
		final String INCLUDES = "http://health.mil/ontologies/Relation/Includes/";
		final String OUTPUT = "http://health.mil/ontologies/Relation/Output/";
		final String BELONGS_TO = "http://health.mil/ontologies/Relation/BelongsTo/";
		final String TAGGED_BY = "http://health.mil/ontologies/Relation/TaggedBy/";
		final String INFLUENCES = "http://health.mil/ontologies/Relation/Influences/";
		final String TYPE_OF = "http://health.mil/ontologies/Relation/TypeOf/";
		final String ESTIMATED = "http://health.mil/ontologies/Relation/Estimated/";
		
		//list of properties
		final String FACTOR = "http://semoss.org/ontologies/Relation/Contains/Factor";
		
		// note that all "ser" in these keys now represent future icds
		// DataObject -> input -> DesignGLItem
		Vector<String[]> data = allData.get("Data-DesignGLItem");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DATA_OBJECT, INPUT, DESIGN_GLITEM); 
		
		// TestGLItem -> includes -> GLItemCoreTask
		data = allData.get("TestGLItem-GLItemCT");
		data.remove(0);
		data.remove(0);
		insertRelData(data, TEST_GLITEM, INCLUDES, GLITEM_CORE_TASK);
		
		// RequirementsGLItem -> output -> ICD
		data = allData.get("RequirementsGLItem-Ser");
		data.remove(0);
		data.remove(0);
		insertRelData(data, REQUIREMENTS_GLITEM, OUTPUT, ICD);
		
		// DevelopGLItem -> output -> ICD
		data = allData.get("DevelopGLItem-Ser");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DEVELOP_GLITEM, OUTPUT, ICD);
		
		// TestGLItem -> belongs to -> SDLCPhase
		data = allData.get("TestGLItem-Phase");
		data.remove(0);
		data.remove(0);
		insertRelData(data, TEST_GLITEM, BELONGS_TO, PHASE);

		// DesignGLItem -> tagged by -> GLTag
		data = allData.get("DesignGLItemTag");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DESIGN_GLITEM, TAGGED_BY, GLTAG);

		// System -> influences -> DesignGLItem
		data = allData.get("Sys-DesignGLItem");
		data.remove(0);
		data.remove(0);
		// need to get list of all systems to make into ActiveSystems
		insertRelDataAndGetSystems(data, SYSTEM, INFLUENCES, DESIGN_GLITEM);

		// RequirementsGLItem -> includes -> GLItemCoreTask
		data = allData.get("RequirementsGLItem-GLItemCT");
		data.remove(0);
		data.remove(0);
		insertRelData(data, REQUIREMENTS_GLITEM, INCLUDES, GLITEM_CORE_TASK);

		// GLItemCoreTask -> includes -> GLItemSubTask
		data = allData.get("GLItemCT-GLItemST");
		data.remove(0);
		data.remove(0);
		insertRelData(data, GLITEM_CORE_TASK, INCLUDES, GLITEM_SUB_TASK);

		// GLItemSubTask -> contains/factor -> factor
		data = allData.get("GLItemSubTaskProp");
		data.remove(0);
		data.remove(0);
		insertPropData(data, GLITEM_SUB_TASK, FACTOR);

		// DesignGLItem -> output -> icd
		data = allData.get("DesignGLItem-Ser");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DESIGN_GLITEM, OUTPUT, ICD);

		// GLItemCoreTask -> type of -> TargetPhaseBasisCoreTask
		data = allData.get("GLItemCT-BasisCT");
		data.remove(0);
		data.remove(0);
		insertRelData(data, GLITEM_CORE_TASK, TYPE_OF, TARGET_PHASE_BASIS_CORE_TASK);

		// GLItemSubTask -> estimated -> TargetPhaseBasisSubTaskComplexityComplexity
		data = allData.get("GLItemST-STBasisCompComp");
		data.remove(0);
		data.remove(0);
		insertRelData(data, GLITEM_SUB_TASK, ESTIMATED, TARGET_PHASE_BASIS_SUB_TASK_COMPLEXITY_COMPLEXITY);

		// RequirementsGLItem -> tagged by -> GLTag
		data = allData.get("RequirementsGLItemTag");
		data.remove(0);
		data.remove(0);
		insertRelData(data, REQUIREMENTS_GLITEM, TAGGED_BY, GLTAG);

		// DevelopGLItem -> belongs to -> SDLCPhase
		data = allData.get("DevelopGLItem-Phase");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DEVELOP_GLITEM, BELONGS_TO, PHASE);
		
		// TestGLItem -> tagged by -> GLTag
		data = allData.get("TestGLItemTag");
		data.remove(0);
		data.remove(0);
		insertRelData(data, TEST_GLITEM, TAGGED_BY, GLTAG);

		// RequirementsGLItem -> belongs to -> SDLCPhase
		data = allData.get("RequirementsGLItem-Phase");
		data.remove(0);
		data.remove(0);
		insertRelData(data, REQUIREMENTS_GLITEM, BELONGS_TO, PHASE);

		// DataObject -> input -> RequirementsGLItem
		data = allData.get("Data-RequirementsGLItem");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DATA_OBJECT, INPUT, REQUIREMENTS_GLITEM);
		
		// DataObject -> input -> TestGLItem
		data = allData.get("Data-TestGLItem"); 
		data.remove(0);
		data.remove(0);
		insertRelData(data, DATA_OBJECT, INPUT, TEST_GLITEM);
		
		// DesignGLItem -> includes -> GLItemCoreTask
		data = allData.get("DesignGLItem-GLItemCT");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DESIGN_GLITEM, INCLUDES, GLITEM_CORE_TASK);

		// System -> influences -> DevelopGLItem
		data = allData.get("Sys-DevelopGLItem");
		data.remove(0);
		data.remove(0);
		insertRelData(data, SYSTEM, INFLUENCES, DEVELOP_GLITEM);
		
		// DataObject -> input -> DevelopGLItem
		data = allData.get("Data-DevelopGLItem");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DATA_OBJECT, INPUT, DEVELOP_GLITEM);

		// DesignGLItem -> belongs to -> SDLCPhase
		data = allData.get("DesignGLItem-Phase");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DESIGN_GLITEM, BELONGS_TO, PHASE);

		// DevelopGLItem -> tagged by -> GLTag
		data = allData.get("DevelopGLItemTag");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DEVELOP_GLITEM, TAGGED_BY, GLTAG);

		// DevelopGLItem -> includes -> GLItemCoreTask
		data = allData.get("DevelopGLItem-GLItemCT");
		data.remove(0);
		data.remove(0);
		insertRelData(data, DEVELOP_GLITEM, INCLUDES, GLITEM_CORE_TASK);

		// System -> influences -> RequirementsGLItem
		data = allData.get("Sys-RequirementsGLItem");
		data.remove(0);
		data.remove(0);
		insertRelData(data, SYSTEM, INFLUENCES, REQUIREMENTS_GLITEM);

		// TestGLItem -> output -> ICD
		data = allData.get("TestGLItem-Ser");
		data.remove(0);
		data.remove(0);
		insertRelData(data, TEST_GLITEM, OUTPUT, ICD);

		// System -> influences -> TestGLItem
		data = allData.get("Sys-TestGLItem");
		data.remove(0);
		data.remove(0);
		insertRelData(data, SYSTEM, INFLUENCES, TEST_GLITEM);

	}
	
	public void insertPropData(Vector<String[]> data, String subBaseURI, String propURI) {
		dataHash.clear();
		allConcepts.clear();
		allRelations.clear();
		
		int size = data.size();
		List<Object[]> newData = new Vector<Object[]>(size);
		
		int i;
		for(i = 0; i < size; i++) {
			String[] rel = data.get(i);
			String sub = Utility.cleanString(rel[0], true);
			Object retObj = null;
			String objAsString = rel[1];
			try {
				Object obj = Double.parseDouble(objAsString);
				retObj = obj;
			} catch(NumberFormatException ex) {
				retObj = objAsString;
			}
			
			String subURI = subBaseURI.concat(sub);
			newData.add(new Object[]{subURI, propURI, retObj});
		}
		
		processInstancePropOnNodeData(newData, futureCostDB);
		processData(futureCostDB, dataHash);
		// process the high lvl node data
		processAllConceptTypeTriples(futureCostDB);
		// process the high lvl rel data
		processAllRelationshipSubpropTriples(futureCostDB);
	}
	
	private void addLOEcalc() {
		// must commit queries to be able to query and add the loe's
		((BigDataEngine) futureCostDB).commit();
		((BigDataEngine) futureCostDB).infer();
		
		ISelectWrapper sjsw = Utility.processQuery(futureCostDB, insertLOEQuery);
		String[] names = sjsw.getVariables();
		while(sjsw.hasNext()) {
			ISelectStatement sjss = sjsw.next();
			String sub = sjss.getRawVar(names[0]).toString();
			String pred = sjss.getRawVar(names[1]).toString();
			Double obj = (Double) sjss.getVar(names[2]);
			( (BigDataEngine) futureCostDB).addStatement(new Object[]{sub, pred, obj, false});
		}
		
		( (BigDataEngine) futureCostDB).addStatement(new Object[]{"http://semoss.org/ontologies/Relation/Contains/LOECalc", RDF.TYPE.toString(), "http://semoss.org/ontologies/Relation/Contains", false});
	}
	
	private void addGLItemSubclassing() {
		String concept = "http://semoss.org/ontologies/Concept";
		String subclassOf = RDFS.SUBCLASSOF.toString();
		
		( (BigDataEngine) futureCostDB).addStatement(new Object[]{"http://semoss.org/ontologies/Concept/GLItem", subclassOf, concept, true});
		( (BigDataEngine) futureCostDB).addStatement(new Object[]{"http://semoss.org/ontologies/Concept/TransitionGLItem", subclassOf, concept, true});

		( (BigDataEngine) futureCostDB).addStatement(new Object[]{"http://semoss.org/ontologies/Concept/RequirementsGLItem", subclassOf, "http://semoss.org/ontologies/Concept/TransitionGLItem", true});
		( (BigDataEngine) futureCostDB).addStatement(new Object[]{"http://semoss.org/ontologies/Concept/DesignGLItem", subclassOf, "http://semoss.org/ontologies/Concept/TransitionGLItem", true});
		( (BigDataEngine) futureCostDB).addStatement(new Object[]{"http://semoss.org/ontologies/Concept/DevelopGLItem", subclassOf, "http://semoss.org/ontologies/Concept/TransitionGLItem", true});
		( (BigDataEngine) futureCostDB).addStatement(new Object[]{"http://semoss.org/ontologies/Concept/TestGLItem", subclassOf, "http://semoss.org/ontologies/Concept/TransitionGLItem", true});
		( (BigDataEngine) futureCostDB).addStatement(new Object[]{"http://semoss.org/ontologies/Concept/DeployGLItem", subclassOf, "http://semoss.org/ontologies/Concept/TransitionGLItem", true});
	}
	
	public void insertRelDataAndGetSystems(Vector<String[]> data, String subBaseURI, String predBaseURI, String objBaseURI) {
		dataHash.clear();
		allConcepts.clear();
		allRelations.clear();
		
		Set<String> sysList = new HashSet<String>();
		
		int size = data.size();
		List<Object[]> newData = new Vector<Object[]>(size);
		
		int i;
		for(i = 0; i < size; i++) {
			String[] rel = data.get(i);
			String sub = Utility.cleanString(rel[0], true);
			String obj = Utility.cleanString(rel[1], true);
			String predURI = predBaseURI.concat(sub).concat(":").concat(obj);
			String subURI = subBaseURI.concat(sub);
			sysList.add(subURI);
			String objURI = objBaseURI.concat(obj);
			
			newData.add(new String[]{subURI, predURI, objURI});
		}
		processInstanceDataRelations(newData, baseFutureCostRelations);
		processData(futureCostDB, dataHash);
		// process the high lvl node data
		processAllConceptTypeTriples(futureCostDB);
		// process the high lvl rel data
		processAllRelationshipSubpropTriples(futureCostDB);
		processActiveSystemSubclassing(futureDB, sysList);
	}
	
	public void insertRelData(Vector<String[]> data, String subBaseURI, String predBaseURI, String objBaseURI) {
		dataHash.clear();
		allConcepts.clear();
		allRelations.clear();
		
		int size = data.size();
		List<Object[]> newData = new Vector<Object[]>(size);
		
		int i;
		for(i = 0; i < size; i++) {
			String[] rel = data.get(i);
			String sub = Utility.cleanString(rel[0], true);
			String obj = Utility.cleanString(rel[1], true);
			String predURI = predBaseURI.concat(sub).concat(":").concat(obj);
			String subURI = subBaseURI.concat(sub);
			String objURI = objBaseURI.concat(obj);
			
			newData.add(new String[]{subURI, predURI, objURI});
		}
		processInstanceDataRelations(newData, baseFutureCostRelations);
		processData(futureCostDB, dataHash);
		// process the high lvl node data
		processAllConceptTypeTriples(futureCostDB);
		// process the high lvl rel data
		processAllRelationshipSubpropTriples(futureCostDB);
	}
	
	private void prepareGLItemGenerator(GLItemGeneratorICDValidated generator){
		generator.setCoreEngine(hrCore);
		generator.fillSystemComplexityHash();
		
		generator.setCoreEngine(futureDB);
		generator.genSDLCVector();
		generator.prepareAllDataHash();
		generator.setGenSpecificDProtQuery(this.genSpecificDProtQuery);
		generator.setGenSpecificDFormQuery(this.genSpecificDFormQuery);
		generator.setProviderDataQuery1(this.providerDataQuery1);
		generator.setProviderDataQuery2(this.providerDataQuery2);
		generator.setConsumerDataQuery(this.consumerDataQuery);

	}
	
	private void runGenerator(GLItemGeneratorICDValidated generator){
		generator.fillProviderDataList();
		generator.fillConsumerDataList();
		
		generator.genSpecificProtocol();
		generator.genProviderDataList();
		generator.genConsumerDataList();
		generator.genCoreTasks();
		generator.genFactors();
	}
	
}
