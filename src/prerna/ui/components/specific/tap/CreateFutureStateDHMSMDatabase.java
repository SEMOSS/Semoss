package prerna.ui.components.specific.tap;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.rdfxml.util.RDFXMLPrettyWriter;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
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
	
	public CreateFutureStateDHMSMDatabase(IEngine hrCore, IEngine futureState, IEngine futureCostDB) {
		this.hrCore = hrCore;
		this.futureState = futureState;
		this.futureCostState = futureCostDB;
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
		addToOWL(futureCostState, baseFutureCostRelations);
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
		addToOWL(futureState, baseFutureRelations);
		// update base filter hash
		((AbstractEngine) futureState).createBaseRelationEngine();
	}
	
	public void processInstanceDataRelations(ArrayList<Object[]> data, HashMap<String, HashMap<String, Set<String>>> baseRelations) {
		for(Object[] triple: data){
			createBaseRelationsHash(triple, baseRelations);
			addToDataHash(triple);
			addToAllConcepts(triple[0].toString());
			addToAllRelationships(triple[1].toString());
			if(triple[1].toString().contains("/Concept/")) {
				System.out.println(":error");
			}
			addToAllConcepts(triple[2].toString());
		}
	}
	
	public void processInstancePropOnRelationshipData(ArrayList<Object[]> data, IEngine engine){
		Set<String> storePropURI = new HashSet<String>();
		for(Object[] triple: data){
			storePropURI.add(triple[1].toString());
			addToDataHash(triple);
			addToAllRelationships(triple[0].toString());
		}
		//add http://semoss.org/ontology/Relation/Contains/PropName -> RDF:TYPE -> http://semoss.org/ontology/Relation/Contains
		for(String propURI: storePropURI) {
			processNewConceptsAtInstanceLevel(engine, propURI, semossPropertyBaseURI.substring(0, semossPropertyBaseURI.length()-1));
		}
	}
	
	public void processInstancePropOnNodeData(ArrayList<Object[]> data, IEngine engine){
		Set<String> storePropURI = new HashSet<String>();
		for(Object[] triple: data){
			storePropURI.add(triple[1].toString());
			addToDataHash(triple);
			addToAllConcepts(triple[0].toString());
		}
		//add http://semoss.org/ontology/Relation/Contains/PropName -> RDF:TYPE -> http://semoss.org/ontology/Relation/Contains
		for(String propURI: storePropURI) {
			processNewConceptsAtInstanceLevel(engine, propURI, semossPropertyBaseURI.substring(0, semossPropertyBaseURI.length()-1));
		}
	}
	
	public void processActiveSystemSubclassing(IEngine engine, Set<String> data){
		processNewSubclass(engine, "http://semoss.org/ontologies/Concept/System", "http://semoss.org/ontologies/Concept/ActiveSystem");
		for(String sysURI : data) {
			processNewConceptsAtInstanceLevel(engine, sysURI, "http://semoss.org/ontologies/Concept/ActiveSystem");
		}
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
	
	public void createBaseRelationsHash(Object[] triple, HashMap<String, HashMap<String, Set<String>>> baseRelations) {
		String subjectBaseURI = semossConceptBaseURI + Utility.getClassName(triple[0].toString());
		String predicateBaseURI = semossRelationBaseURI + Utility.getClassName(triple[1].toString());
		String objectBaseURI = semossConceptBaseURI + Utility.getClassName(triple[2].toString());
		if(baseRelations.containsKey(subjectBaseURI)) {
			HashMap<String, Set<String>> innerHash = baseRelations.get(subjectBaseURI);
			if(innerHash.containsKey(predicateBaseURI)) {
				innerHash.get(predicateBaseURI).add(objectBaseURI);
			} else {
				Set<String> list = new HashSet<String>();
				list.add(objectBaseURI);
				innerHash.put(predicateBaseURI, list);
			}
		} else {
			Set<String> list = new HashSet<String>();
			list.add(objectBaseURI);
			HashMap<String, Set<String>> innerHash = new HashMap<String, Set<String>>();
			innerHash.put(predicateBaseURI, list);
			baseRelations.put(subjectBaseURI, innerHash);
		}
	}
	
	public void addToOWL(IEngine engine, HashMap<String, HashMap<String, Set<String>>> baseRelations) throws RepositoryException, RDFHandlerException 
	{
		// get the path to the owlFile
		String owlFileLocation = DIHelper.getInstance().getProperty(engine.getEngineName() +"_" + Constants.OWL); 

		RDFFileSesameEngine existingBaseEngine = (RDFFileSesameEngine) ( (AbstractEngine) engine).getBaseDataEngine();
		for(String subjectURI : baseRelations.keySet()) 
		{
			HashMap<String, Set<String>> predicateURIHash = baseRelations.get(subjectURI);
			for(String predicateURI : predicateURIHash.keySet()) 
			{
				Set<String> objectURIList = predicateURIHash.get(predicateURI);
				for(String objectURI : objectURIList) 
				{
					existingBaseEngine.addStatement(subjectURI, predicateURI, objectURI, true);
				}
			}
		}
		
		RepositoryConnection exportRC = existingBaseEngine.getRc();
		FileWriter fWrite = null;
		try{
			fWrite = new FileWriter(owlFileLocation);
			RDFXMLPrettyWriter owlWriter  = new RDFXMLPrettyWriter(fWrite); 
			exportRC.export(owlWriter);
			fWrite.flush();
			owlWriter.close();	
		}
		catch(IOException ex)
		{
			ex.printStackTrace();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			e.printStackTrace();
		}finally{
			try{
				if(fWrite!=null)
					fWrite.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void generateData() throws EngineException {
		LPInterfaceProcessor processor = new LPInterfaceProcessor();
		processor.setEngine(hrCore);
		processor.isGenerateCost(true);
		processor.setGenerateNewTriples(true);
		processor.setUsePhase(true);
		if(tapCost == null) {
			tapCost = (IEngine) DIHelper.getInstance().getLocalProp("TAP_Cost_Data");
			if(tapCost == null) {
				throw new EngineException("Cost Info Not Found");
			}
		}
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
		SesameJenaSelectWrapper sjsw = Utility.processQuery(futureState, CURR_ICD_AND_WEIGHT_QUERY);
		String[] varNames = sjsw.getVariables();
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
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
