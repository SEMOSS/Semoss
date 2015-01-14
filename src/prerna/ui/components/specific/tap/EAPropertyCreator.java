package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.HashMap;

import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.BigDataEngine;

public class EAPropertyCreator extends AggregationHelper {
	
	private IEngine hrCore;
	private ArrayList<Object[]> effectPropList;
	private ArrayList<Object[]> efficiencyPropList;
	private ArrayList<Object[]> productPropList;
	
	private final String semossPropURI = "http://semoss.org/ontologies/Relation/Contains/";
	private final String bpInstanceRel = "http://health.mil/ontologies/Concept/BusinessProcess/";
	
	public EAPropertyCreator(IEngine hrCore) {
		this.hrCore = hrCore;
	}
	
	private void addPropTriples() {
		effectPropList = new ArrayList<Object[]>();
		efficiencyPropList = new ArrayList<Object[]>();
		productPropList = new ArrayList<Object[]>();
		HashMap<String, Double> effectMap = new HashMap<String, Double>();
		HashMap<String, Double> efficiencyMap = new HashMap<String, Double>();
		HashMap<String, Double> productMap = new HashMap<String, Double>();
		
		EABenefitsSchedulePlaySheet percentages = new EABenefitsSchedulePlaySheet();
		percentages.setQuery("");
		percentages.createData();
		percentages.runAnalytics();
		effectMap = percentages.effectPercentMap;
		efficiencyMap = percentages.efficiencyPercentMap;
		productMap = percentages.productivityPercentMap;
		
		for (String bp : effectMap.keySet()) {
			String bpURI = bpInstanceRel.concat(bp);
			Object[] values = new Object[] { bpURI, semossPropURI.concat("EA-Effectiveness"), effectMap.get(bp) };
			effectPropList.add(values);
		}
		for (String bp : efficiencyMap.keySet()) {
			String bpURI = bpInstanceRel.concat(bp);
			Object[] values = new Object[] { bpURI, semossPropURI.concat("EA-Efficiency"), efficiencyMap.get(bp) };
			efficiencyPropList.add(values);
		}
		for (String bp : productMap.keySet()) {
			String bpURI = bpInstanceRel.concat(bp);
			Object[] values = new Object[] { bpURI, semossPropURI.concat("EA-Productivity"), productMap.get(bp) };
			productPropList.add(values);
		}
	}
	
	public void addProperties() throws EngineException, RepositoryException, RDFHandlerException {
		dataHash.clear();
		allConcepts.clear();
		allRelations.clear();
		
		addPropTriples();
		processInstancePropOnNodeData(effectPropList, hrCore);
		processInstancePropOnNodeData(efficiencyPropList, hrCore);
		processInstancePropOnNodeData(productPropList, hrCore);
		processData(hrCore, dataHash);
		((BigDataEngine) hrCore).commit();
		((BigDataEngine) hrCore).infer();
		// update base filter hash
		((AbstractEngine) hrCore).createBaseRelationEngine();
	}
}
