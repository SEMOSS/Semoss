/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;

/**
 * This class is used to generate items in the GL and is used to update the cost database.
 */
public class GLItemGeneratorSelfReportedFutureInterfaces extends AggregationHelper{

	private IEngine hrCore;
	private IEngine futureDB;
	private IEngine futureCostDB;
	private IEngine tapCost;

	private String genSpecificDProtQuery = "SELECT DISTINCT ?icd ?data ?sys (COALESCE(?dprot, (URI(\"http://health.mil/ontologies/Concept/DProt/HTTPS-SOAP\"))) AS ?Prot) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} OPTIONAL{ ?payload <http://semoss.org/ontologies/Relation/Contains/Protocol> ?dprot} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String genSpecificDFormQuery = "SELECT DISTINCT ?icd ?data ?sys (COALESCE(?dform, (URI(\"http://health.mil/ontologies/Concept/DForm/XML\"))) AS ?Form) WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} OPTIONAL{ ?payload <http://semoss.org/ontologies/Relation/Contains/Format> ?dform} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String providerDataQuery1 = "SELECT DISTINCT ?icd ?data ?sys WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Provide> AS ?upstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?sys ?upstream ?icd ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;}  FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	private String providerDataQuery2 = "";
	private String consumerDataQuery = "SELECT DISTINCT ?icd ?data ?sys WHERE { {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}  {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}BIND(<http://semoss.org/ontologies/Relation/Consume> AS ?downstream) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;} {?icd ?downstream ?sys ;} {?payload <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload> } {?icd ?payload ?data ;} FILTER(STR(?sys)!=\"http://health.mil/ontologies/Concept/System/DHMSM\")}";
	
	private HashMap<String, HashMap<String, Set<String>>> baseFutureCostRelations = new HashMap<String, HashMap<String, Set<String>>>();
	
	/**
	 * Constructor for GLItemGeneratorSelfReportedFutureInterfaces.
	 */
	public GLItemGeneratorSelfReportedFutureInterfaces(IEngine hrCore, IEngine futureState, IEngine futureCostDB, IEngine costDB) {
		this.hrCore = hrCore;
		this.futureDB = futureState;
		this.futureCostDB = futureCostDB;
		this.tapCost = costDB;
	}

	public void genData() throws RepositoryException, RDFHandlerException{
		GLItemGeneratorICDValidated generator = new GLItemGeneratorICDValidated();
		prepareGLItemGenerator(generator);
		runGenerator(generator);
		Hashtable<String, Vector<String[]>> allData = generator.getAllDataHash();
		getData(allData);
		
//		((BigDataEngine) futureCostDB).commit();
//		((BigDataEngine) futureCostDB).infer();
//		writeToOWL(futureCostDB, baseFutureCostRelations);
//		// update base filter hash
//		((AbstractEngine) futureCostDB).createBaseRelationEngine();
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
		insertRelData(data, DATA_OBJECT, INPUT, DESIGN_GLITEM); //TODO: check is this is actually data
		
		// TestGLItem -> includes -> GLItemCoreTask
		data = allData.get("TestGLItem-GLItemCT");
		insertRelData(data, TEST_GLITEM, INCLUDES, GLITEM_CORE_TASK);
		
		// RequirementsGLItem -> output -> ICD
		data = allData.get("RequirementsGLItem-Ser");
		insertRelData(data, REQUIREMENTS_GLITEM, OUTPUT, ICD); //TODO: check if service is replaced by icd
		
		// DevelopGLItem -> output -> ICD
		data = allData.get("DevelopGLItem-Ser");
		insertRelData(data, DEVELOP_GLITEM, OUTPUT, ICD); //TODO: check if service is replaced by icd
		
		// TestGLItem -> belongs to -> SDLCPhase
		data = allData.get("TestGLItem-Phase");
		insertRelData(data, TEST_GLITEM, BELONGS_TO, PHASE);

		// DesignGLItem -> tagged by -> GLTag
		data = allData.get("DesignGLItemTag");
		insertRelData(data, DESIGN_GLITEM, TAGGED_BY, GLTAG);

		// System -> influences -> DesignGLItem
		data = allData.get("Sys-DesignGLItem");
		insertRelData(data, SYSTEM, INFLUENCES, DESIGN_GLITEM);

		// RequirementsGLItem -> includes -> GLItemCoreTask
		data = allData.get("RequirementsGLItem-GLItemCT");
		insertRelData(data, REQUIREMENTS_GLITEM, INCLUDES, GLITEM_CORE_TASK);

		// GLItemCoreTask -> includes -> GLItemSubTask
		data = allData.get("GLItemCT-GLItemST");
		insertRelData(data, GLITEM_CORE_TASK, INCLUDES, GLITEM_SUB_TASK);

		// GLItemSubTask -> contains/factor -> factor
		data = allData.get("GLItemSubTaskProp");
		insertPropData(data, GLITEM_SUB_TASK, FACTOR);

		// DesignGLItem -> output -> icd
		data = allData.get("DesignGLItem-Ser");
		insertRelData(data, DESIGN_GLITEM, OUTPUT, ICD); //TODO: check if service is replaced by icd

		// GLItemCoreTask -> type of -> TargetPhaseBasisCoreTask
		data = allData.get("GLItemCT-BasisCT");
		insertRelData(data, GLITEM_CORE_TASK, TYPE_OF, TARGET_PHASE_BASIS_CORE_TASK);

		// GLItemSubTask -> estimated -> TargetPhaseBasisSubTaskComplexityComplexity
		data = allData.get("GLItemST-STBasisCompComp");
		insertRelData(data, GLITEM_SUB_TASK, ESTIMATED, TARGET_PHASE_BASIS_SUB_TASK_COMPLEXITY_COMPLEXITY);

		// RequirementsGLItem -> tagged by -> GLTag
		data = allData.get("RequirementsGLItemTag");
		insertRelData(data, REQUIREMENTS_GLITEM, TAGGED_BY, GLTAG);

		// DevelopGLItem -> belongs to -> SDLCPhase
		data = allData.get("DevelopGLItem-Phase");
		insertRelData(data, DEVELOP_GLITEM, BELONGS_TO, PHASE);
		
		// TestGLItem -> tagged by -> GLTag
		data = allData.get("TestGLItemTag");
		insertRelData(data, TEST_GLITEM, TAGGED_BY, GLTAG);

		// RequirementsGLItem -> belongs to -> SDLCPhase
		data = allData.get("RequirementsGLItem-Phase");
		insertRelData(data, REQUIREMENTS_GLITEM, BELONGS_TO, PHASE);

		// DataObject -> input -> RequirementsGLItem
		data = allData.get("Data-RequirementsGLItem");
		insertRelData(data, DATA_OBJECT, INPUT, REQUIREMENTS_GLITEM); //TODO: check is this is actually data
		
		// DataObject -> input -> TestGLItem
		data = allData.get("Data-TestGLItem"); 
		insertRelData(data, DATA_OBJECT, INPUT, TEST_GLITEM); //TODO: check is this is actually data
		
		// DesignGLItem -> includes -> GLItemCoreTask
		data = allData.get("DesignGLItem-GLItemCT");
		insertRelData(data, DESIGN_GLITEM, INCLUDES, GLITEM_CORE_TASK);

		// System -> influences -> DevelopGLItem
		data = allData.get("Sys-DevelopGLItem");
		insertRelData(data, SYSTEM, INFLUENCES, DEVELOP_GLITEM);
		
		// DataObject -> input -> DevelopGLItem
		data = allData.get("Data-DevelopGLItem");
		insertRelData(data, DATA_OBJECT, INPUT, DEVELOP_GLITEM);

		// DesignGLItem -> belongs to -> SDLCPhase
		data = allData.get("DesignGLItem-Phase");
		insertRelData(data, DESIGN_GLITEM, BELONGS_TO, PHASE);

		// TestGLItem -> tagged by -> GLTag
		data = allData.get("DevelopGLItemTag");
		insertRelData(data, TEST_GLITEM, TAGGED_BY, GLTAG);

		// DevelopGLItem -> includes -> GLItemCoreTask
		data = allData.get("DevelopGLItem-GLItemCT");
		insertRelData(data, DEVELOP_GLITEM, INCLUDES, GLITEM_CORE_TASK);

		// System -> influences -> RequirementsGLItem
		data = allData.get("Sys-RequirementsGLItem");
		insertRelData(data, SYSTEM, INFLUENCES, REQUIREMENTS_GLITEM);

		// TestGLItem -> output -> ICD
		data = allData.get("TestGLItem-Ser");
		insertRelData(data, TEST_GLITEM, OUTPUT, ICD); //TODO: check if service is replaced by icd

		// System -> influences -> TestGLItem
		data = allData.get("Sys-TestGLItem");
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
			String sub = rel[0];
			Object obj = rel[1];
			String subURI = subBaseURI.concat(sub);
			
			newData.add(new Object[]{subURI, propURI, obj});
		}
		
		processInstancePropOnNodeData(newData, futureCostDB);
		processData(futureCostDB, dataHash);
		// process the high lvl node data
		processAllConceptTypeTriples(futureCostDB);
		// process the high lvl rel data
		processAllRelationshipSubpropTriples(futureCostDB);
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
			String sub = rel[0];
			String obj = rel[1];
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
