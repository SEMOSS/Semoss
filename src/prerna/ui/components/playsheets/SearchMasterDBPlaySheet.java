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
package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.impl.SearchMasterDB;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;

/**
 * The SearchMasterDBPlaySheet class is used to test the Search feature for the MasterDB.
 */
@SuppressWarnings("serial")
public class SearchMasterDBPlaySheet extends GridPlaySheet{
	
	Hashtable<String, SEMOSSVertex> vertStore = new Hashtable<String, SEMOSSVertex>();
	Hashtable<String, SEMOSSEdge> edgeStore = new Hashtable<String, SEMOSSEdge>();
	ArrayList<SEMOSSVertex> instanceList = new ArrayList<SEMOSSVertex>();

	
	/**
	 * Method createData.  Creates the data needed to be printout in the grid.
	 */
	@Override
	public void createData() {
		SearchMasterDB searchAlgo = new SearchMasterDB();
		searchAlgo.setMasterDBName(this.engine.getEngineName());
		
		if(query.contains("true"))
			searchAlgo.isDebugging(true);
		else
			searchAlgo.isDebugging(false);
		
		if(query.contains("1")){
			createMetamodelSubgraphData();
			searchAlgo.setKeywordAndEdgeList(vertStore, edgeStore, true);
		} else if(query.contains("2")) {
			createInstanceSubgraphData();
			searchAlgo.setKeywordAndEdgeList(vertStore, edgeStore, false);
		} else if(query.contains("3")) {
			createInstanceSubgraphData2();
			searchAlgo.setInstanceList(instanceList);
			searchAlgo.setKeywordAndEdgeList(vertStore, edgeStore, false);
		}
		
		ArrayList<Hashtable<String, Object>> hashArray = searchAlgo.searchDB();
		flattenHash(hashArray);
	}
	
	private void flattenHash(ArrayList<Hashtable<String, Object>> hashArray){
		//TODO write this method that stores headers and list
	}
	
	private void createMetamodelSubgraphData() {
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore = new Hashtable<String, SEMOSSEdge>();
		
		SEMOSSVertex icd = new SEMOSSVertex("http://semoss.org/ontologies/Concept/InterfaceControlDocument");
		SEMOSSVertex data = new SEMOSSVertex("http://semoss.org/ontologies/Concept/DataObject");
		SEMOSSVertex service = new SEMOSSVertex("http://semoss.org/ontologies/Concept/Service");
		SEMOSSVertex system = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System");
		SEMOSSVertex dataElement = new SEMOSSVertex("http://semoss.org/ontologies/Concept/DataElement");
		vertStore.put(icd.uri,icd);
		vertStore.put(data.uri,data);
		vertStore.put(service.uri,service);
		vertStore.put(system.uri,system);
		vertStore.put(dataElement.uri,dataElement);
		
		SEMOSSEdge icdData = new SEMOSSEdge(icd,data,"http://semoss.org/ontologies/Relation/Payload/InterfaceControlDocument:DataObject");
		SEMOSSEdge serviceICD = new SEMOSSEdge(service,icd,"http://semoss.org/ontologies/Relation/Payload/Service:InterfaceControlDocument");
		SEMOSSEdge systemData = new SEMOSSEdge(system,data,"http://semoss.org/ontologies/Relation/Payload/System:DataObject");
		SEMOSSEdge icdSystem = new SEMOSSEdge(icd,system,"http://semoss.org/ontologies/Relation/Payload/InterfaceControlDocument:System");
		SEMOSSEdge systemICD = new SEMOSSEdge(system,icd,"http://semoss.org/ontologies/Relation/Payload/System:InterfaceControlDocument");
		SEMOSSEdge serviceData = new SEMOSSEdge(service,data,"http://semoss.org/ontologies/Relation/Payload/Service:DataObject");
		SEMOSSEdge dataEleData = new SEMOSSEdge(dataElement,data,"http://semoss.org/ontologies/Relation/Payload/DataElement:DataObject");
		edgeStore.put(icdData.getURI(), icdData);
		edgeStore.put(serviceICD.getURI(), serviceICD);
		edgeStore.put(systemData.getURI(), systemData);
		edgeStore.put(icdSystem.getURI(), icdSystem);
		edgeStore.put(systemICD.getURI(), systemICD);
		edgeStore.put(serviceData.getURI(), serviceData);
		edgeStore.put(dataEleData.getURI(), dataEleData);
	}
	private void createInstanceSubgraphData() {
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore = new Hashtable<String, SEMOSSEdge>();
		
		SEMOSSVertex icd1 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/InterfaceControlDocument/CDR-AHLTA-Referral_Information");
		SEMOSSVertex icd2 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/InterfaceControlDocument/BHIE-AHLTA-Referral_Information");
		SEMOSSVertex data = new SEMOSSVertex("http://semoss.org/ontologies/Concept/DataObject/Referral_Information");
		SEMOSSVertex system1 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/CDR");
		SEMOSSVertex system2 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/AHLTA");
		SEMOSSVertex system3 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/BHIE");
		
		vertStore.put(icd1.uri,icd1);
		vertStore.put(icd2.uri,icd2);
		vertStore.put(data.uri,data);
		vertStore.put(system1.uri,system1);
		vertStore.put(system2.uri,system2);
		vertStore.put(system3.uri,system3);
		
		SEMOSSEdge icd1Data = new SEMOSSEdge(icd1,data,"http://health.mil/ontologies/Relation/Payload/CDR-AHLTA-Referral_Information:Referral_Information");
		SEMOSSEdge system1ICD1 = new SEMOSSEdge(system1,icd1,"http://health.mil/ontologies/Relation/Provide/CDR:CDR-AHLTA-Referral_Information");
		SEMOSSEdge icdSystem2 = new SEMOSSEdge(icd1,system2,"http://health.mil/ontologies/Relation/Consume/CDR-AHLTA-Referral_Information:AHLTA");
		
		SEMOSSEdge icd2Data = new SEMOSSEdge(icd2,data,"http://health.mil/ontologies/Relation/Payload/CDR-AHLTA-Referral_Information:Referral_Information");
		SEMOSSEdge system3ICD2 = new SEMOSSEdge(system3,icd2,"http://health.mil/ontologies/Relation/Provide/BHIE:BHIE-AHLTA-Referral_Information");
		SEMOSSEdge icd2System2 = new SEMOSSEdge(icd2,system2,"http://health.mil/ontologies/Relation/Consume/BHIE-AHLTA-Referral_Information:AHLTA");
		
		edgeStore.put(icd1Data.getURI(), icd1Data);
		edgeStore.put(system1ICD1.getURI(), system1ICD1);
		edgeStore.put(icdSystem2.getURI(), icdSystem2);
		edgeStore.put(icd2Data.getURI(), icd2Data);
		edgeStore.put(system3ICD2.getURI(), system3ICD2);
		edgeStore.put(icd2System2.getURI(), icd2System2);
	}
	private void createInstanceSubgraphData2() {
		
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore = new Hashtable<String, SEMOSSEdge>();
		
		SEMOSSVertex system = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/AHLTA");
		vertStore.put(system.uri,system);
		SEMOSSVertex data = new SEMOSSVertex("http://semoss.org/ontologies/Concept/DataObject/Referral_Information");
		vertStore.put(data.uri,data);
		
		instanceList.add(system);
		instanceList.add(data);

//		vertStore = new Hashtable<String, SEMOSSVertex>();
//		edgeStore = new Hashtable<String, SEMOSSEdge>();
//		
//		SEMOSSVertex icd1 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/InterfaceControlDocument/CDR-AHLTA-Referral_Information");
//		SEMOSSVertex data = new SEMOSSVertex("http://semoss.org/ontologies/Concept/DataObject/Referral_Information");
//		SEMOSSVertex system1 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/CDR");
//		SEMOSSVertex system2 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/AHLTA");
//		
//		vertStore.put(icd1.uri,icd1);
//		vertStore.put(data.uri,data);
//		vertStore.put(system1.uri,system1);
//		vertStore.put(system2.uri,system2);
//		
//		SEMOSSEdge icd1Data = new SEMOSSEdge(icd1,data,"http://health.mil/ontologies/Relation/Payload/CDR-AHLTA-Referral_Information:Referral_Information");
//		SEMOSSEdge system1ICD1 = new SEMOSSEdge(system1,icd1,"http://health.mil/ontologies/Relation/Provide/CDR:CDR-AHLTA-Referral_Information");
//		SEMOSSEdge icdSystem2 = new SEMOSSEdge(icd1,system2,"http://health.mil/ontologies/Relation/Consume/CDR-AHLTA-Referral_Information:AHLTA");
//
//		edgeStore.put(icd1Data.getURI(), icd1Data);
//		edgeStore.put(system1ICD1.getURI(), system1ICD1);
//		edgeStore.put(icdSystem2.getURI(), icdSystem2);
//		
//		keywordList.add("System");
//		instanceList.add("AHLTA");		
//		
//		keywordList.add("DataObject");
//		instanceList.add("Referral_Information");		
	}
}
