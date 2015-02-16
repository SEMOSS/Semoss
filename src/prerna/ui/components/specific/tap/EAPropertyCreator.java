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
