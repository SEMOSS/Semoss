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
import java.util.StringTokenizer;

import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
/**
 * This class performs the business value calculations for a capability.
 */
public class CapabilityBVCalculationPerformer implements IAlgorithm,Runnable{

	public Hashtable<String, Object> BVhash = new Hashtable<String, Object>();
	
	static final Logger logger = LogManager.getLogger(CapabilityBVCalculationPerformer.class.getName());
	GridFilterData gfd = new GridFilterData();
	
	/**
	 * Constructor for CapabilityBVCalculationPerformer.
	 */
	public CapabilityBVCalculationPerformer() {
		
	}

	/**
	 * Processes the query used to run the calculation for business value.
	 */
	public void runCalculation() {
		try {
			fillHash();
			
			if (DIHelper.getInstance().getLocalProp(Constants.CAPABILITY_BUSINESS_VALUE)!=null) 
				BVhash = (Hashtable<String, Object>) DIHelper.getInstance().getLocalProp(Constants.CAPABILITY_BUSINESS_VALUE);

			calculateBusinessProcessBV();
			calculateCapabilityBV();
			DIHelper.getInstance().setLocalProperty(Constants.CAPABILITY_BUSINESS_VALUE, BVhash);
			
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			Object[] repos = (Object [])list.getSelectedValues();
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[0]+"");
			
			Hashtable capBVHash = (Hashtable) DIHelper.getInstance().getLocalProp(Constants.CAPABILITY_BUSINESS_VALUE);
			ArrayList<Double> capFutureBVs = (ArrayList<Double>) capBVHash.get(Constants.CAPABILITY_BUSINESS_VALUE+"Future"+Constants.CALC_MATRIX);
			ArrayList<Double> capRealizedBVs =(ArrayList<Double>) capBVHash.get(Constants.CAPABILITY_BUSINESS_VALUE+"Realized"+Constants.CALC_MATRIX);
			ArrayList<String> capList = (ArrayList<String>) capBVHash.get(Constants.CAPABILITY_BUSINESS_VALUE+Constants.CALC_NAMES_LIST);

			//finds any capability that is not in the current result list and adds a bv of 0 to it.		
			String query = "SELECT DISTINCT ?Capability WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}}";
			
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			
			/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
			*/
			String[] names = wrapper.getVariables();

			while(wrapper.hasNext())
			{
				ISelectStatement sjss= wrapper.next();
				String capability = (String)sjss.getVar(names[0]);
				if(!capList.contains(capability))
				{
					capList.add(capability);
					capFutureBVs.add(0.0);
					capRealizedBVs.add(0.0);
				}
			}
			
			query = prepareInsert("Capability",capList, capFutureBVs, "FutureBusinessValue");
			UpdateProcessor pro = new UpdateProcessor();
			pro.setQuery(query);
			pro.processQuery();

			query = prepareInsert("Capability",capList, capRealizedBVs, "RealizedBusinessValue");
			pro = new UpdateProcessor();
			pro.setQuery(query);
			pro.processQuery();
		
			ArrayList<Double> bpFutureBVs = (ArrayList<Double>) capBVHash.get(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+"Future"+Constants.CALC_MATRIX);
			ArrayList<Double> bpRealizedBVs =(ArrayList<Double>) capBVHash.get(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+"Realized"+Constants.CALC_MATRIX);
			ArrayList<String> bpList = (ArrayList<String>) capBVHash.get(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+Constants.CALC_NAMES_LIST);

			//finds any business process that is not in the current result list and adds a bv of 0 to it.		
			query = "SELECT DISTINCT ?BusinessProcess WHERE {{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;}}";			
	
			
			wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

			/*wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();*/
			names = wrapper.getVariables();

			while(wrapper.hasNext())
			{
				ISelectStatement sjss= wrapper.next();
				String bp = (String)sjss.getVar(names[0]);
				if(!bpList.contains(bp))
				{
					bpList.add(bp);
					bpFutureBVs.add(0.0);
					bpRealizedBVs.add(0.0);
				}
			}
			
			query = prepareInsert("BusinessProcess",bpList, bpFutureBVs, "FutureBusinessValue");
			pro = new UpdateProcessor();
			pro.setQuery(query);
			pro.processQuery();

			query = prepareInsert("BusinessProcess",bpList, bpRealizedBVs, "RealizedBusinessValue");
			pro = new UpdateProcessor();
			pro.setQuery(query);
			pro.processQuery();
			
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}

	/**
	 * Runs the business value calculations.
	 */
	public void run() {
		runCalculation();
	}
	
	/**
	 * Runs the queries on the set engine to calculate business value.
	 */
	private void fillHash(){

		String MultipleQueries = "SELECT DISTINCT ?capability ?support ?businessprocess ?weight WHERE { {?capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?support <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports>;} {?businessprocess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}{?capability ?support ?businessprocess;} {?support <http://semoss.org/ontologies/Relation/Contains/weight> ?weight ;}} ORDER BY ?businessprocess+++SELECT DISTINCT ?businessprocess ?consist ?activity WHERE { {?businessprocess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}{?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;}{?businessprocess ?consist ?activity;} }ORDER BY ?businessprocess+++SELECT DISTINCT ?activity ?assigned ?ferror ?weight WHERE { {?activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;}{?assigned <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned>;} {?ferror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError>;}{?assigned <http://semoss.org/ontologies/Relation/Contains/weight> ?weight ;}{?activity ?assigned ?ferror;} }+++SELECT DISTINCT ?businessprocess ?prop ?efficiency ?transcount ?criticality WHERE{BIND(\"prop\" AS ?prop){?businessprocess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;}{?businessprocess <http://semoss.org/ontologies/Relation/Contains/Efficiency> ?efficiency}{?businessprocess <http://semoss.org/ontologies/Relation/Contains/Transactions_Num> ?transcount}{?businessprocess <http://semoss.org/ontologies/Relation/Contains/Wartime_Criticality> ?criticality}}ORDER BY ?businessprocess";

		StringTokenizer queryTokens = new StringTokenizer(MultipleQueries, "+++");
		while(queryTokens.hasMoreElements()){
			String query = (String) queryTokens.nextElement();
			
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			Object[] repos = (Object [])list.getSelectedValues();
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[0]+"");
			logger.info("Repository is " + repos);
			FillCapabilityBVHash filler = new FillCapabilityBVHash(query, (IEngine)engine);

			Thread playThread = new Thread(filler);
			playThread.run();
		}
	}
	
	/**
	 * Calculates bv for a given business process. A score is calculated for each activity related to the process.
	 * The activity score is based upon the rate of functional gaps and the efficiency of the business process.
	 * The activity scores are summed to then create the business process bv. This information is stored in the bv business process hash.
	 * */
	public void calculateBusinessProcessBV(){
		double[][] bpAct = (double[][])BVhash.get("BusinessProcess/Activity"+Constants.CALC_MATRIX);
		ArrayList<String> bpActRowLabels = (ArrayList<String>)BVhash.get("BusinessProcess/Activity"+Constants.CALC_ROW_LABELS);
		ArrayList<String> bpActColLabels = (ArrayList<String>)BVhash.get("BusinessProcess/Activity"+Constants.CALC_COLUMN_LABELS);
		
		double[][] actFerror = (double[][])BVhash.get("Activity/FError"+Constants.CALC_MATRIX);
		ArrayList<String> actFerrorRowLabels = (ArrayList<String>)BVhash.get("Activity/FError"+Constants.CALC_ROW_LABELS);
		ArrayList<String> actFerrorColLabels = (ArrayList<String>)BVhash.get("Activity/FError"+Constants.CALC_COLUMN_LABELS);

		double[][] bpProp = (double[][])BVhash.get("BusinessProcess/prop"+Constants.CALC_MATRIX);
		ArrayList<String> bpPropRowLabels = (ArrayList<String>)BVhash.get("BusinessProcess/prop"+Constants.CALC_ROW_LABELS);
		
		ArrayList<Double> bpRealizedBV = new ArrayList<Double>();
		ArrayList<Double> bpFutureBV = new ArrayList<Double>();
		double bpWeightDenom = 0;			
		
		//run calculation for every business process
		for(int bpIndex = 0;bpIndex < bpAct.length;bpIndex++)
		{
			String bpLabel = bpActRowLabels.get(bpIndex);
			int bpPropRowIndex = bpPropRowLabels.indexOf(bpLabel);
			double bpRealizedScore=0;
			double bpFutureScore=0;
			
			//get all data needed for bpWeighting
			double bpEfficiency = 0;
			double bpTransnum = 0;
			double bpCriticality = 0;
			if(bpPropRowIndex>=0)
			{
				bpEfficiency = bpProp[bpPropRowIndex][0];
				bpTransnum = bpProp[bpPropRowIndex][1];
				bpCriticality = bpProp[bpPropRowIndex][2];
			}
			//calculate score without weight
			for(int actColIndex=0;actColIndex<bpAct[0].length;actColIndex++)
				if(bpAct[bpIndex][actColIndex]==1)
				{
					String actLabel = bpActColLabels.get(actColIndex);
					int actRowIndex = actFerrorRowLabels.indexOf(actLabel);
					if(actRowIndex>=0)
						for(int ferrorColIndex=0;ferrorColIndex<actFerror[0].length;ferrorColIndex++)
						{
							double ferrorRate = actFerror[actRowIndex][ferrorColIndex];
							if(ferrorRate>0)
							{
								bpFutureScore+=(1-((1-ferrorRate)*bpEfficiency));
								bpRealizedScore+=((1-ferrorRate)*bpEfficiency);
							}
						}
				}
			
			//calculate numerator portion of weight and multiply. add to update the denominator poriton of weight.
			//add bp bv (will divide all by denom portion of weight in next for loop
			double bpWeightNum=0;
			if(bpTransnum!=0)
				bpWeightNum = bpCriticality*Math.log(bpTransnum);
			bpRealizedScore = bpRealizedScore*bpWeightNum;
			bpFutureScore = bpFutureScore*bpWeightNum;
			bpWeightDenom+=bpWeightNum;
			bpFutureBV.add(bpFutureScore);
			bpRealizedBV.add(bpRealizedScore);
		
		}
		//divide all values by denominator to make the weighting
		for(int i=0;i<bpRealizedBV.size();i++)
		{
			if(bpWeightDenom==0.0)
			{
				bpRealizedBV.set(i,0.0);
				bpFutureBV.set(i,0.0);
			}
			else
			{
				bpRealizedBV.set(i,bpRealizedBV.get(i)/bpWeightDenom);
				bpFutureBV.set(i,bpFutureBV.get(i)/bpWeightDenom);
			}
		}
		BVhash.put(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+"Future"+Constants.CALC_MATRIX, bpFutureBV);
		BVhash.put(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+"Realized"+Constants.CALC_MATRIX, bpRealizedBV);
		BVhash.put(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+Constants.CALC_NAMES_LIST, bpActRowLabels);
	}
	
	/**
	 * Calculates business value by summing and weighting the results from the business process bv scores.
	 * */
	public void calculateCapabilityBV(){
		double[][] capBP = (double[][])BVhash.get("Capability/BusinessProcess"+Constants.CALC_MATRIX);
		ArrayList<String> capBPRowLabels = (ArrayList<String>)BVhash.get("Capability/BusinessProcess"+Constants.CALC_ROW_LABELS);
		ArrayList<String> capBPColLabels = (ArrayList<String>)BVhash.get("Capability/BusinessProcess"+Constants.CALC_COLUMN_LABELS);
		
		ArrayList<Double> bpRealizedBVs = (ArrayList<Double>) BVhash.get(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+"Realized"+Constants.CALC_MATRIX);
		ArrayList<Double> bpFutureBVs = (ArrayList<Double>) BVhash.get(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+"Future"+Constants.CALC_MATRIX);
		ArrayList<String> bpBVRowLabels = (ArrayList<String>) BVhash.get(Constants.BUSINESS_PROCESS_BUSINESS_VALUE+Constants.CALC_NAMES_LIST);

		ArrayList<Double> capRealizedBVs = new ArrayList<Double>();
		ArrayList<Double> capFutureBVs = new ArrayList<Double>();
		//run calculation for capability
		for(int capIndex = 0;capIndex < capBP.length;capIndex++)
		{
			double capRealizedBV=0;
			double capFutureBV=0;
			for(int bpColIndex=0;bpColIndex<capBP[0].length;bpColIndex++)
			{
				double capBPWeight = capBP[capIndex][bpColIndex];
				if(capBPWeight>0)
				{
					String bpLabel = capBPColLabels.get(bpColIndex);
					int bpBVRowIndex = bpBVRowLabels.indexOf(bpLabel);
					double bpRealizedBV = 0;
					double bpFutureBV = 0;
					if(bpBVRowIndex>=0)
					{
						bpFutureBV=bpFutureBVs.get(bpBVRowIndex);
						bpRealizedBV=bpRealizedBVs.get(bpBVRowIndex);
					}
					capRealizedBV+=bpRealizedBV*capBPWeight;
					capFutureBV+=bpFutureBV*capBPWeight;
				}
			}
			capFutureBVs.add(capFutureBV);
			capRealizedBVs.add(capRealizedBV);

		}	
		BVhash.put(Constants.CAPABILITY_BUSINESS_VALUE+"Realized"+Constants.CALC_MATRIX, capRealizedBVs);
		BVhash.put(Constants.CAPABILITY_BUSINESS_VALUE+"Future"+Constants.CALC_MATRIX, capFutureBVs);
		BVhash.put(Constants.CAPABILITY_BUSINESS_VALUE+Constants.CALC_NAMES_LIST, capBPRowLabels);
	}
	

	/**
	 * Given a set of parameters, creates the query to be inserted.
	 * @param nodeType	Node type string that business value should be added to
	 * @param names 	List of capability names.
	 * @param values 	List of business values associated with capabilities.
	 * @param propName 	Property name string used to create the predicate (relation) URI.
	
	 * @return String 	Query to be inserted. */
	public String prepareInsert(String nodeType, ArrayList<String> names, ArrayList<Double> values, String propName){
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
			String subjectUri = "<http://health.mil/ontologies/Concept/"+nodeType+"/"+names.get(sysIdx) +">";
			String objectUri = "\"" + values.get(sysIdx) + "\"" + "^^<http://www.w3.org/2001/XMLSchema#double>";
			insertQuery = insertQuery + subjectUri + " " + predUri + " " + objectUri + ". ";
		}
		insertQuery = insertQuery + "}";
		
		return insertQuery;
	}
	

	/**
	 * Sets the playsheet.
	 * @param graphPlaySheet IPlaySheet
	 */
	@Override
	public void setPlaySheet(IPlaySheet graphPlaySheet) {
		
	}


	/**
	 * Gets the variables.
	// TODO: Don't return null
	 * @return String[] */
	@Override
	public String[] getVariables() {
		
		return null;
	}


	/**
	 * Executes the algorithm.
	 */
	@Override
	public void execute() {
		
	}


	/**
	 * Gets the algorithm name.
	// TODO: Don't return null
	 * @return String */
	@Override
	public String getAlgoName() {
		
		return null;
	}
	
}
