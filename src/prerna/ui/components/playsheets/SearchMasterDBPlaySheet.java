/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.components.playsheets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;

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
//		searchAlgo.setMasterDBName(this.engine.getEngineName());
		
		ArrayList<Hashtable<String, Object>> hashArray = new ArrayList<Hashtable<String, Object>>();
		if(query.contains("1")){
			createMetamodelSubgraphData();
			searchAlgo.setKeywordAndEdgeList(vertStore, edgeStore, true);
			hashArray = searchAlgo.findRelatedEngines();
			flattenHash(hashArray);
		} else if(query.contains("2")) {//engines must have instances but doesn't return questions
			createInstanceSubgraphData();
			searchAlgo.setInstanceList(instanceList);
			searchAlgo.setKeywordAndEdgeList(vertStore, edgeStore, false);
			hashArray = searchAlgo.findRelatedEngines();
			flattenHash(hashArray);
		} else if(query.contains("3")) {//engines must have instances and returns questions
			createInstanceSubgraphData2();
			searchAlgo.setInstanceList(instanceList);
			searchAlgo.setKeywordAndEdgeList(vertStore, edgeStore, false);
			hashArray = searchAlgo.findRelatedQuestions();
			flattenHash(hashArray);
		}
	}

	private void flattenHash(ArrayList<Hashtable<String, Object>> hashArray){
		//TODO write this method that stores headers and list
		//assuming every hash has the same keys
		//get the first hash to know what keys we are working with (these are going to be our headers)
		if(hashArray.size()>0)
		{
			list = new ArrayList<Object[]>();
			Hashtable<String, Object> exampleHash = hashArray.get(0);
			Collection<String> keySet = exampleHash.keySet();
			this.names = new String[keySet.size()];
			Iterator<String> keyIt = keySet.iterator();
			for(int namesIdx = 0; keyIt.hasNext(); namesIdx++){
				this.names[namesIdx] = keyIt.next();
			}
			
			// now that names has been created, just need to fill out the list to match the headers
			for(Hashtable<String,Object> hash : hashArray){
				Object[] newRow = new Object[this.names.length];
				for(int namesIdx = 0; namesIdx<this.names.length; namesIdx++)
				{
					newRow[namesIdx] = hash.get(this.names[namesIdx]);
				}
				list.add(newRow);
			}
				
		}
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
		
		SEMOSSVertex system2 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/AHLTA");

		vertStore.put(system2.uri,system2);
		instanceList.add(system2);

	}
	private void createInstanceSubgraphData2() {
		
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore = new Hashtable<String, SEMOSSEdge>();
		
		SEMOSSVertex data = new SEMOSSVertex("http://semoss.org/ontologies/Concept/DataObject/Referral_Information");		
		SEMOSSVertex icd1 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/InterfaceControlDocument/CDR-AHLTA-Referral_Information");
		SEMOSSVertex system1 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/CDR");
		SEMOSSVertex system2 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/System/AHLTA");
		
		vertStore.put(icd1.uri,icd1);
		vertStore.put(data.uri,data);
		vertStore.put(system1.uri,system1);
		vertStore.put(system2.uri,system2);
		vertStore.put(data.uri,data);
		
		instanceList.add(system1);
		instanceList.add(data);

		SEMOSSEdge icd1Data = new SEMOSSEdge(icd1,data,"http://health.mil/ontologies/Relation/Payload/CDR-AHLTA-Referral_Information:Referral_Information");
		SEMOSSEdge system1ICD1 = new SEMOSSEdge(system1,icd1,"http://health.mil/ontologies/Relation/Provide/CDR:CDR-AHLTA-Referral_Information");
		SEMOSSEdge icdSystem2 = new SEMOSSEdge(icd1,system2,"http://health.mil/ontologies/Relation/Consume/CDR-AHLTA-Referral_Information:AHLTA");

		edgeStore.put(icd1Data.getURI(), icd1Data);
		edgeStore.put(system1ICD1.getURI(), system1ICD1);
		edgeStore.put(icdSystem2.getURI(), icdSystem2);
	}
}
