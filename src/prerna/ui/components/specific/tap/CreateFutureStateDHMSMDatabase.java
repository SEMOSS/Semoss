package prerna.ui.components.specific.tap;

import java.util.ArrayList;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;

public class CreateFutureStateDHMSMDatabase extends AggregationHelper {

	private final String newICDTypeName = "http://semoss.org/ontologies/Relation/ProposedInterfaceControlDocument";
	private final String removedICDTypeName = "http://semoss.org/ontologies/Relation/ProposedDecommissionedInterfaceControlDocument";
	
	private IEngine hrCore;
	private IEngine futureStateHrCore;
	
	private ArrayList<Object[]> relList;
	private ArrayList<Object[]> propList;
	private ArrayList<String> addedInterfaces;
	private ArrayList<String> removedInterfaces;
	
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
	
	public void createNewDB() throws EngineException {
		if(relList == null || propList == null || addedInterfaces == null || removedInterfaces == null) {
			generateData();
		}
		
		for(Object[] triple: relList){
			addToDataHash(triple);
			addToAllConcepts(triple[0].toString());
			addToAllRelationships(triple[1].toString());
			addToAllConcepts(triple[2].toString());
		}
		
		for(Object[] triple: propList){
			addToDataHash(triple);
			addToAllConcepts(triple[0].toString());
		}
		
	}

	public void generateData() throws EngineException {
		LPInterfaceProcessor processor = new LPInterfaceProcessor();
		processor.setEngine(hrCore);
		processor.setGenerateNewTriples(true);
		processor.generateReport();
		relList = processor.getRelList();
		propList = processor.getPropList();
		addedInterfaces = processor.getAddedInterfaces();
		removedInterfaces = processor.getRemovedInterfaces();
	}
	
}
