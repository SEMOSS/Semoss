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
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class CreateFutureStateDHMSMDatabase extends AggregationHelper {

	private final String newICDTypeName = "http://semoss.org/ontologies/Concept/ProposedInterfaceControlDocument";
	private final String removedICDTypeName = "http://semoss.org/ontologies/Concept/ProposedDecommissionedInterfaceControlDocument";
	private final String icdType = "http://semoss.org/ontologies/Concept/InterfaceControlDocument";
	
	private IEngine hrCore;
	private IEngine futureStateHrCore;
	
	private ArrayList<Object[]> relList;
	private ArrayList<Object[]> relPropList;
	private ArrayList<String> addedInterfaces;
	private ArrayList<String> removedInterfaces;
	private Set<String> sysList;
	
	private HashMap<String, HashMap<String, Set<String>>> baseRelations;
	
	public CreateFutureStateDHMSMDatabase() {

	}
	
	public CreateFutureStateDHMSMDatabase(IEngine hrCore, IEngine futureStateHrCore) {
		this.hrCore = hrCore;
		this.futureStateHrCore = futureStateHrCore;
	}
	
	public void setHrCore(IEngine hrCore) {
		this.hrCore = hrCore;
	}
	
	public void setFutureStateHrCore(IEngine futureStateHrCore) {
		this.futureStateHrCore = futureStateHrCore;
	}
	
	public void createNewDB() throws EngineException, RepositoryException, RDFHandlerException {
		if(relList == null || relPropList == null || addedInterfaces == null || removedInterfaces == null) {
			generateData();
		}
		
		baseRelations = new HashMap<String, HashMap<String, Set<String>>>();

		// process through triples
		for(Object[] triple: relList){
			createBaseRelationsHash(triple);
			addToDataHash(triple);
			addToAllConcepts(triple[0].toString());
			addToAllRelationships(triple[1].toString());
			addToAllConcepts(triple[2].toString());
		}
		
		Set<String> storePropURI = new HashSet<String>();
		for(Object[] triple: relPropList){
			storePropURI.add(triple[1].toString());
			addToDataHash(triple);
			addToAllRelationships(triple[0].toString());
		}
		//add http://semoss.org/ontology/Relation/Contains/PropName -> RDF:TYPE -> http://semoss.org/ontology/Relation/Contains
		for(String propURI: storePropURI) {
			processNewConceptsAtInstanceLevel(futureStateHrCore, propURI, semossPropertyBaseURI.substring(0, semossPropertyBaseURI.length()-1));
		}
		
		processData(futureStateHrCore, dataHash);

		// process the high lvl node data
		for(String newConcept : allConcepts.keySet()) {
			processNewConcepts(futureStateHrCore, newConcept);
			Set<String> instanceSet = allConcepts.get(newConcept);
			for(String newInstance : instanceSet) {
				processNewConceptsAtInstanceLevel(futureStateHrCore, newInstance, newConcept);
			}
		}
		// process the high lvl rel data
		for(String newRelationship : allRelations.keySet()) {
			processNewRelationships(futureStateHrCore, newRelationship);
			Set<String> instanceSet = allRelations.get(newRelationship);
			for(String newRelInstance : instanceSet) {
				processNewRelationshipsAtInstanceLevel(futureStateHrCore, newRelInstance, newRelationship);
			}
		}
		
		//add sub-classing for systems
		processNewSubclass(futureStateHrCore, "http://semoss.org/ontologies/Concept/System", "http://semoss.org/ontologies/Concept/ActiveSystem");
		for(String sysURI : sysList) {
			processNewConceptsAtInstanceLevel(futureStateHrCore, sysURI, "http://semoss.org/ontologies/Concept/ActiveSystem");
		}
		
		// add sub-classing of icd's
		processNewSubclass(futureStateHrCore, icdType, newICDTypeName);
		processNewSubclass(futureStateHrCore, icdType, removedICDTypeName);
		for(String addedICD: addedInterfaces) {
			processNewConceptsAtInstanceLevel(futureStateHrCore, addedICD, newICDTypeName);
		}
		for(String removedICD: removedInterfaces) {
			processNewConceptsAtInstanceLevel(futureStateHrCore, removedICD, removedICDTypeName);
		}
		
		((BigDataEngine) futureStateHrCore).commit();
		((BigDataEngine) futureStateHrCore).infer();
		addToOWL();
		// update base filter hash
		((AbstractEngine) futureStateHrCore).createBaseRelationEngine();
	}

	public void generateData() throws EngineException {
		LPInterfaceProcessor processor = new LPInterfaceProcessor();
		processor.setEngine(hrCore);
		processor.setGenerateNewTriples(true);
		processor.generateReport();
		relList = processor.getRelList();
		relPropList = processor.getPropList();
		addedInterfaces = processor.getAddedInterfaces();
		removedInterfaces = processor.getRemovedInterfaces();
		sysList = processor.getSysList();
	}
	
	public void createBaseRelationsHash(Object[] triple) {
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
	
	public void addToOWL() throws RepositoryException, RDFHandlerException 
	{
		// get the path to the owlFile
		String owlFileLocation = DIHelper.getInstance().getProperty(futureStateHrCore.getEngineName() +"_" + Constants.OWL); 

		RDFFileSesameEngine existingBaseEngine = (RDFFileSesameEngine) ( (AbstractEngine) futureStateHrCore).getBaseDataEngine();
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
	
}
