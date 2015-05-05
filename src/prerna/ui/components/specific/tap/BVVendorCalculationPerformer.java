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
import java.util.Hashtable;
import java.util.StringTokenizer;

import prerna.algorithm.api.IAlgorithm;
import prerna.engine.api.IEngine;
import prerna.ui.components.UpdateProcessor;
import prerna.util.Constants;
import prerna.util.DIHelper;
/**
 * This class extends the functionality of BVCalculationPerformer but is used specifically for vendors.
 */
public class BVVendorCalculationPerformer extends BVCalculationPerformer implements IAlgorithm,Runnable{

	/**
	 * Runs the business value calculations by running the BV query on a specified engine.
	 */
	public void runCalculation() {
		logger.info("Graph PlaySheet Begining Calculation");
		Hashtable paramHash = new Hashtable();
		if(!type.equals("Vendor"))
		{
			double networkBetaValue = 1-soaAlphaValue;
			paramHash.put("soaAlphaValue", Double.toString(soaAlphaValue));
			paramHash.put("networkBetaValue", Double.toString(networkBetaValue));
		}
		try {
			fillHash(paramHash);
			if (DIHelper.getInstance().getLocalProp(Constants.BUSINESS_VALUE)!=null) 
				BVhash = (Hashtable<String, Object>) DIHelper.getInstance().getLocalProp(Constants.BUSINESS_VALUE);
//			logger.info(BVhash.keySet());
			calculateActivitySystemErrAdj();
			calculateBPasis();
			calculateBusinessValue();
			
			//printMatrix("Activity/BLU/Split", type);
			//testPrint("Activity/IO/Split/Service");
			DIHelper.getInstance().setLocalProperty(type+Constants.BUSINESS_VALUE, BVhash);
			//printToGridPlaySheet(type+Constants.BUSINESS_VALUE);
			
			ArrayList<Double> businessValues = new ArrayList<Double>();
			ArrayList<String> businessNames = new ArrayList<String>();
			Hashtable BVHash = (Hashtable) DIHelper.getInstance().getLocalProp(type+Constants.BUSINESS_VALUE);
			businessValues = (ArrayList<Double>) BVHash.get(type+Constants.BUSINESS_VALUE+Constants.CALC_MATRIX);
			businessNames = (ArrayList<String>) BVHash.get(type+Constants.BUSINESS_VALUE+Constants.CALC_NAMES_LIST);

			String businessQuery = prepareInsert(businessNames, businessValues, "BusinessValue");
			
			UpdateProcessor pro = new UpdateProcessor();
			pro.setQuery(businessQuery);
			pro.processQuery();
			
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Calculates business value.
	 */
	public void run() {
		logger.info("Graph PlaySheet Begining Calculation");
		Hashtable paramHash = new Hashtable();
		if(!type.equals("Vendor"))
		{
			double networkBetaValue = 1-soaAlphaValue;
			paramHash.put("soaAlphaValue", Double.toString(soaAlphaValue));
			paramHash.put("networkBetaValue", Double.toString(networkBetaValue));
		}
		try {
			fillHash(paramHash);
			if (DIHelper.getInstance().getLocalProp(Constants.BUSINESS_VALUE)!=null) 
				BVhash = (Hashtable<String, Object>) DIHelper.getInstance().getLocalProp(Constants.BUSINESS_VALUE);
			calculateActivitySystemErrAdj();
			calculateBPasis();
			calculateBusinessValue();
			//printToGridPlaySheet(type+Constants.BUSINESS_VALUE);
			//testPrint(type+Constants.BUSINESS_VALUE);
			//printMatrix(type, "DataObject");
			//alphabetizeBVhash();
			//fillBottomMatrices();
			DIHelper.getInstance().setLocalProperty(Constants.BUSINESS_VALUE, BVhash);
			
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}
	
	/**
	 * Method fillHash.
	 * @param paramHash Hashtable
	 */
	private void fillHash(Hashtable paramHash){
		//String MultipleQueries = this.sparql.getText();
		String MultipleQueries = "SELECT ?activity1 ?need ?data ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?activity1 ?need ?data ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT DISTINCT ?vendor ?SatisfiesDataObject ?data ?FulfillLevel WHERE {{?Dataconcat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/VendorSystemCapabilityTaskDataObjectService>;}{?AssociatedTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/AssociatedTo> ;}{?concat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/VendorSystemCapabilityTask>;}{?Dataconcat ?AssociatedTo ?concat;}{?vendor <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor> ;}{?HasVendor <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}{?concat ?HasVendor ?vendor;}{?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?SatisfiesDataObject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Satisfies> ;}{?Dataconcat ?SatisfiesDataObject ?data;}{?FulfillLevel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SupportLevel> ;}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports> ;}{?Dataconcat ?Supports ?FulfillLevel;}}+++SELECT ?activity1 ?need ?blu ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?activity1 ?need ?blu ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT DISTINCT ?vendor ?SatisfiesBLU ?blu ?FulfillLevel WHERE {{?BLUconcat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/VendorSystemCapabilityTaskBusinessLogicUnitService>;}{?AssociatedTo <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/AssociatedTo> ;}{?concat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/VendorSystemCapabilityTask>;}{?BLUconcat ?AssociatedTo ?concat;}{?vendor <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor> ;}{?HasVendor <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;}{?concat ?HasVendor ?vendor;}{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?SatisfiesBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Satisfies> ;}{?BLUconcat ?SatisfiesBLU ?blu;}{?FulfillLevel <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SupportLevel> ;}{?Supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports> ;}{?BLUconcat ?Supports ?FulfillLevel;}} +++SELECT ?act ?have ?element ?weight1 WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ActivitySplit> ;} {?element <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Element> ;} {?act ?have ?element ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?sys ?have ?terror ?weight1 WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;} {?terror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError> ;} {?sys ?have ?terror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?act ?have ?ferror ?weight1 WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned> ;} {?ferror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError> ;} {?act ?have ?ferror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?bp ?consist ?act ?contains2 ?props WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>;} {?bp ?consist ?act ;} {?bp ?contains2 ?props}}+++SELECT ?bp ?consist ?act ?weight1 WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?bp ?consist ?act ;} {?consist <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}";

		//does sys terror need to be here?
		
		StringTokenizer queryTokens = new StringTokenizer(MultipleQueries, "+++");
		while(queryTokens.hasMoreElements()){
			String query = (String) queryTokens.nextElement();
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("HR_Core");
			if(query.toLowerCase().contains("vendor"))
				engine = (IEngine)DIHelper.getInstance().getLocalProp("VendorSelection");
			logger.info("Repository is " + engine);
			FillBVHash filler = new FillBVHash(query, (IEngine)engine);

			Thread playThread = new Thread(filler);
			playThread.start();
			
		}
	}
	
	/**
	 * Given the calculations for business process activities and adjusted activity type errors, 
	 * calculate the business value and put it in the BV hash.
	
	 * @return ArrayList<Double>	List of calculated business values. */
	public ArrayList<Double> calculateBusinessValue(){
		double[][] processSystem = multiply("BPasis/Final", type+"ActivityTypeErrAdj/Final");
		ArrayList<Double> result = new ArrayList<Double>();
		for (int i = 0; i<processSystem[0].length;i++){
			double total=0;
			for (int j =0; j<processSystem.length; j++){
				total = total + processSystem[j][i];
			}
			result.add(total*10);
		}
		BVhash.put(type+Constants.BUSINESS_VALUE+Constants.CALC_MATRIX, result);
		BVhash.put(type+Constants.BUSINESS_VALUE+Constants.CALC_NAMES_LIST, BVhash.get("BPasis/"+type+"ActivityTypeErrAdj/Final"+Constants.CALC_COLUMN_LABELS));
		logger.info("Calculated BV");
		return result;
	}
	
}
