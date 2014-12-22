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
package prerna.ui.components.specific.tap;

import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.UpdateProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This class is used to process information about ICDs associated with services.
 */
public class ServiceICDProcessor {
	
		static final Logger logger = LogManager.getLogger(ServiceICDProcessor.class.getName());
		public Hashtable finalHash = new Hashtable();
		SesameJenaSelectWrapper sjw = new SesameJenaSelectWrapper();
		public String namesKey = "serviceNames";
		public String valuesKey = "icdCountValues";

		/**
		 * Runs the processor in order to put service names and values along with ICD names and counts into the hashtable.
		 */
		public void runProcessor() {
			runQuery();
			
			// get the bindings from it
			String [] names = sjw.getVariables();
			ArrayList <Double> serValues = new ArrayList();
			ArrayList <String> serNames = new ArrayList();
			
			int count = 0;
			// now get the bindings and generate the data
			try {
				while(sjw.hasNext())
				{
					SesameJenaSelectStatement sjss = sjw.next();
					serNames.add(count, (String) sjss.getVar(names[0]));
					serValues.add(count, (Double) sjss.getVar(names[1]));
					count++;
				}
				finalHash.put(namesKey, serNames);
				finalHash.put(valuesKey, serValues);

				ArrayList<Double> ICDcounts = new ArrayList<Double>();
				ArrayList<String> ICDnames = new ArrayList<String>();
				Hashtable ICDhash = finalHash;
				ICDnames = (ArrayList<String>) ICDhash.get(namesKey);
				ICDcounts = (ArrayList<Double>) ICDhash.get(valuesKey);
			
				String ICDquery = prepareInsert(ICDnames, ICDcounts, "ICDCount");				
				UpdateProcessor pro = new UpdateProcessor();
				pro.setQuery(ICDquery);
				pro.processQuery();	
				
			}catch(RuntimeException e){
				logger.info(e);
			}
		}
		
		/**
		 * Runs the query via the Sesame wrapper.
		 */
		private void runQuery(){
			String query = "SELECT ?ser (COUNT(?icd) AS ?ICDcount) WHERE { {?ser ?exposes ?data } {?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>;} BIND(<http://semoss.org/ontologies/Relation/Exposes> AS ?exposes) {?icd <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument>;} BIND(<http://semoss.org/ontologies/Relation/Payload> AS ?payload) {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?icd ?payload ?data}} GROUP BY ?ser";
			sjw.setQuery(query);
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			// get the selected repository
			Object [] repos = (Object [])list.getSelectedValues();
			
			logger.info("Repository is " + repos);
			
			for(int repoIndex = 0;repoIndex < repos.length;repoIndex++)
			{
				IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[repoIndex]+"");
				// use the layout to load the sheet later
				sjw.setEngine(engine);
				sjw.executeQuery();
			}
		}
		
		/**
		 * Prepares the insert query.
		 * @param names 		Array list of system names.
		 * @param values 		Array list of ICD count values.
		 * @param propName 		Property name (relation) in string form.
		
		 * @return String 		Query to be inserted.*/
		private String prepareInsert(ArrayList<String> names, ArrayList<Double> values, String propName){
			String predUri = "<http://semoss.org/ontologies/Relation/Contains/"+propName+">";
			
			//add start with type triple
			String insertQuery = "INSERT DATA { " +predUri + " <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://semoss.org/ontologies/Relation/Contains>. ";
			
			//add other type triple
			insertQuery = insertQuery + predUri +" <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +
					"<http://www.w3.org/1999/02/22-rdf-syntax-ns#Property>. ";
			
			//add sub property triple -->>>>>>> should probably look into this.... very strange how other properties are set up
			insertQuery = insertQuery + predUri +" <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> " +
					predUri + ". ";
			
			for(int sysIdx = 0; sysIdx<names.size(); sysIdx++){
				String subjectUri = "<http://health.mil/ontologies/Concept/System/"+names.get(sysIdx) +">";
				String objectUri = "\"" + values.get(sysIdx) + "\"" + "^^<http://www.w3.org/2001/XMLSchema#double>";

				insertQuery = insertQuery + subjectUri + " " + predUri + " " + objectUri + ". ";
			}
			insertQuery = insertQuery + "}";
			
			return insertQuery;
		}
}
