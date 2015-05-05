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
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.VertexFilterData;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
/**
 * This class fills the business value hashtable.
 */
public class FillBVHash implements Runnable{

	String query = null;
	IEngine engine = null;
	VertexFilterData filterData = new VertexFilterData();
	Hashtable<String, Object> BVhash = new Hashtable<String, Object>();
	ISelectWrapper wrapper;
	ArrayList <String []> list;
	Hashtable tempSelectHash = new Hashtable();
	static final Logger logger = LogManager.getLogger(FillBVHash.class.getName());
	Logger fileLogger = Logger.getLogger("reportsLogger");
	
	//Necessary items to create a filler
	/**
	 * Constructor for FillBVHash.
	 * @param query String
	 * @param engine IEngine
	 */
	public FillBVHash(String query,
			IEngine engine) {
		this.query = query;
		this.engine = engine;
	}
	
	//Working SELECT Statement: SELECT ?activity1 ?need ?data ?need ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?activity1 ?need ?data ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?system ?provide ?data ?provide ?weight2 WHERE{ {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide  <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?system ?provide ?data ;} {?provide <http://semoss.org/ontologies/Relation/Contains/weight>  ?weight2}}+++SELECT ?activity1 ?need ?blu ?need ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?activity1 ?need ?blu ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?system ?provide ?blu ?provide ?weight2 WHERE { {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide  <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?system ?provide ?blu ;} {?provide <http://semoss.org/ontologies/Relation/Contains/weight> ?weight2}}+++SELECT ?act ?have ?element ?have ?weight1 WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ActivitySplit> ;} {?element <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Element> ;} {?act ?have ?element ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?sys ?have ?terror ?have ?weight1 WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;} {?terror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError> ;} {?sys ?have ?terror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?act ?have ?ferror ?have ?weight1 WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned> ;} {?ferror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError> ;} {?act ?have ?ferror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?bp ?consist ?act ?bp ?contains2 ?props WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>;} {?bp ?consist ?act ;} {?bp ?contains2 ?props}}+++SELECT ?bp ?consist ?act ?consist ?weight1 WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?bp ?consist ?act ;} {?consist <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}
	/**
	 * This is the SELECT statement used to fill the BV hash.
	 * Executes the query in order to fill the appropriate arraylists used to create the final BV hashtable.
	 */
	public void FillWithSelect() {
		if (DIHelper.getInstance().getLocalProp(Constants.BUSINESS_VALUE)!=null) 
			BVhash = (Hashtable<String, Object>) DIHelper.getInstance().getLocalProp(Constants.BUSINESS_VALUE);
		logger.info("Graph PlaySheet " + query);
		
		wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		
		//wrapper = new SesameJenaSelectWrapper();
		list = new ArrayList();
	
		/*wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();*/
		
		
		fillArrayLists();//fills arraylists with names and puts values in 5000x5000 matrix. Stores all in tempSelectHash
			
		try {
			fillSelectBVhash();//uses tempSelectHash to fill BVhash
			DIHelper.getInstance().setLocalProperty(Constants.BUSINESS_VALUE, BVhash);
			
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}

	//columns must be in the order of vertex1, relationship, vertex2, property
	//Will error if column length or row length is greater than 5000
	//Uses SesameJenaSelectWrapper statements to fill matrices and put in tempSelectHash
	/**
	 * Fills arraylists with names using SesameJenaSelectWrapper statements to fill matrices.
	 * Puts values in 5000x5000 matrix. 
	 * Stores all in tempSelectHash.
	 */
	private void fillArrayLists() {
		// get the bindings from it
		String [] names = wrapper.getVariables();
		int count = 0;
		//ArrayList<ArrayList<Double>> weightValues = new ArrayList<ArrayList<Double>>();
		double[][] matrix = new double[5000][5000];
		ArrayList<String> colLabels = new ArrayList<String>();
		ArrayList<String> rowLabels = new ArrayList<String>();
		Hashtable<String, double[]> bpPropHash = new Hashtable<String, double[]>();
		double[] Efficiency = new double[5000];
		double[] InitialEfficiency = new double[5000];
		double[] InitialEffectiveness = new double[5000];
		double[] TransactionsNum = new double[5000];
		double[] WartimeEfficiency = new double[5000];
		bpPropHash.put("Efficiency", Efficiency);
		bpPropHash.put("Initial_Efficiency", InitialEfficiency);
		bpPropHash.put("Initial_Effectiveness", InitialEffectiveness);
		bpPropHash.put("Transactions_Num", TransactionsNum);
		bpPropHash.put("Wartime_Criticality", WartimeEfficiency);
		
		Hashtable<String,Double> options = new Hashtable<String,Double>();
		options.put("Supports_out_of_box", 10.0);
		options.put("Supports_with_configuration", 7.0);
		options.put("Supports_with_customization", 3.0);
		options.put("Does_not_support", 0.0);
		
		Hashtable<String,Double> vendorObjectCount = new Hashtable<String,Double>();
		
		String key = null;
		boolean check = false;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				
				while(sjss==null && wrapper.hasNext()) {
					sjss=wrapper.next();
				}
				//make key
				if(count == 0){
					String vert1type = Utility.getClassName(sjss.getRawVar(names[0]).toString());
					String vert2type = Utility.getClassName(sjss.getRawVar(names[2]).toString());
					/*String vert1uri = sjss.getVar(names[0])+"";
					String vert1type = Utility.getClassName(vert1uri);
					String vert2uri = sjss.getVar(names[2])+"";
					String vert2type = Utility.getClassName(vert2uri);*/
					key = vert1type + "/" + vert2type;
					if(names.length>3){
						if(names[3].contains("NETWORK")) key=key+"NETWORK";
					}
				}
				count++;
				//if it is an edge triple, everything gets added
				String var0 = sjss.getVar(names[0]).toString();
				String var2 = sjss.getVar(names[2]).toString().replaceAll("\"","");
				
/*				String var0 = Utility.getInstanceName(sjss.getVar(names[0])+"");
				String var1 = Utility.getInstanceName(sjss.getVar(names[1])+"");
				String var2 = Utility.getInstanceName(sjss.getVar(names[2])+"").replaceAll("\"", "");*/
				//if BP/Act, need to get the necessary properties associated with the BP
				if(key.equals("BusinessProcess/Activity")&&(sjss.getVar(names[3]) instanceof String)){
					//String var3 = Utility.getInstanceName(sjss.getVar(names[3])+"");
					String var3 = sjss.getVar(names[3]).toString();
					double[] propArray = bpPropHash.get(var3);
					if (propArray != null){
						if (!rowLabels.contains(var0))
							rowLabels.add(var0);
						//figure out where on the matrix the value should get added
						int rowLoc = rowLabels.indexOf(var0);
						//propArray.add(colLoc, (Double) sjss.getVar(names[4]));
						propArray[rowLoc]=(Double) sjss.getVar(names[4]);
						bpPropHash.put(var3, propArray);
						
						//use check to make sure correct BP/Act query is being used
						//one query is to get weights between BP and Act
						//correct query is to get properties on BP
						if (check == false) {
							check = true;
							tempSelectHash.put("BPkey", new double[1]);
						}
					}
				}

				if((sjss.getVar(names[3]) instanceof Double)){
					if (!rowLabels.contains(var0)){
						rowLabels.add(var0);
						//ArrayList<Double> newCol = new ArrayList<Double>();
						//weightValues.add(weightValues.size(), newCol);
					}
					
					if (! colLabels.contains(var2))
						colLabels.add(var2);
					//get column and row location
					int rowLoc = rowLabels.indexOf(var0);
					int colLoc = colLabels.indexOf(var2);
					//put value in the right spot
					//ArrayList<Double> col = weightValues.remove(colLoc);
					//col.add(rowLoc, (Double) sjss.getVar(names[4]));
					//weightValues.add(colLoc, col);
					matrix[rowLoc][colLoc] = (Double) sjss.getVar(names[3]);
					
					errorCheck(key, var0, var2, (Double) sjss.getVar(names[3]) );
				}

			//if it is 
			logger.debug("Creating new Value ");
				
				if(key.toLowerCase().contains("vendor"))
				{
					if (!rowLabels.contains(var0)){
						rowLabels.add(var0);
					}
					if (! colLabels.contains(var2))
						colLabels.add(var2);
					int rowLoc = rowLabels.indexOf(var0);
					int colLoc = colLabels.indexOf(var2);
					String fulfill = sjss.getVar(names[3]).toString();
					String vendorObject=var0+"!vendorObject!"+var2;
					Double fulfillValue=options.get(fulfill);
					Double fulfillCount=1.0;
					if(vendorObjectCount.containsKey(vendorObject))
					{
						fulfillValue+=matrix[rowLoc][colLoc];
						fulfillCount+=vendorObjectCount.get(vendorObject);
					}
					matrix[rowLoc][colLoc] = fulfillValue;
					vendorObjectCount.put(vendorObject,fulfillCount);
				}
			}
			for(String vendorObject : vendorObjectCount.keySet())
			{
				int begIndex=vendorObject.indexOf("!vendorObject!");
				int endIndex=begIndex+14;
				int rowLoc=rowLabels.indexOf(vendorObject.substring(0,begIndex));
				int colLoc=colLabels.indexOf(vendorObject.substring(endIndex));
				matrix[rowLoc][colLoc]=matrix[rowLoc][colLoc]/vendorObjectCount.get(vendorObject);
			}
		}catch (RuntimeException e) {
			logger.fatal(e);
		}
		tempSelectHash.put(key+Constants.CALC_COLUMN_LABELS, colLabels);
		tempSelectHash.put(key+Constants.CALC_ROW_LABELS, rowLabels);
		tempSelectHash.put(key+Constants.CALC_MATRIX, matrix);
		tempSelectHash.put(key + Constants.PROP_HASH, bpPropHash);
	}
	
	/**
	 * Uses tempSelectHash to fill BVhash.
	 */
	private void fillSelectBVhash(){
		//use arraylists to create matrices
		//put in BVhash
		Iterator<String> keys = tempSelectHash.keySet().iterator();
		String key = null;
		//get real key from the keys in tempSelectHash
		try{
			while (keys.hasNext() && key == null){
				String newKey = keys.next();
				//make sure the key is not "BPkey"
				if(newKey.contains("_")){
					StringTokenizer tokens = new StringTokenizer(newKey, "_");
					key = tokens.nextToken();
				}
			}
			//here is a simple transition from the matrix of 5000x5000 to a correctly sized matrix
			//then it is put in BVhash
			ArrayList<String> colLabels = (ArrayList<String>) tempSelectHash.get(key + Constants.CALC_COLUMN_LABELS);
			ArrayList<String> rowLabels = (ArrayList<String>) tempSelectHash.get(key + Constants.CALC_ROW_LABELS);
			double[][] values = (double[][]) tempSelectHash.get(key + Constants.CALC_MATRIX);
			double[][] matrix = new double[rowLabels.size()][colLabels.size()];
			double totalCheck = 0;
//			System.err.println(key + ".......................................................................................................... ");
			for (int col = 0; col < matrix[0].length; col++){
				for(int row = 0; row<matrix.length; row++){
					matrix[row][col] = values[row][col];
					totalCheck = totalCheck + values[row][col];
//					System.err.println(key +";" +colLabels.get(col) + ";" + rowLabels.get(row) +";" + colLabels.get(col) + rowLabels.get(row) +";"+values[row][col]);
				}
			}
			if(totalCheck == 0) {
				logger.warn("Matrix filled with all zeros: " + key);
				fileLogger.info("Matrix filled with all zeros: " + key);
			}
			BVhash.put(key+Constants.CALC_MATRIX, matrix);
			BVhash.put(key+Constants.CALC_COLUMN_LABELS, colLabels);
			BVhash.put(key+Constants.CALC_ROW_LABELS, rowLabels);
//			System.err.println(key + "        " + matrix.toString());
			
			//if the key is BP/Act and it is the correct query, must set BP props
			//Transition from the double[] to an ArrayList<Double> and put in BVhash
			if(key.equals("BusinessProcess/Activity")&&tempSelectHash.containsKey("BPkey")){
				Hashtable<String, double[]> bpPropHash = (Hashtable<String, double[]>) tempSelectHash.get(key+Constants.PROP_HASH);	
				Iterator<String> keyset = bpPropHash.keySet().iterator();
				StringTokenizer keyTokens = new StringTokenizer(key, "/");
				String propKey = keyTokens.nextToken();
				BVhash.put(propKey + Constants.CALC_NAMES_LIST, rowLabels);
				while (keyset.hasNext()){
					String prop = keyset.next();
					double[] propMatrix = bpPropHash.get(prop);
					ArrayList<Double> propArray = new ArrayList<Double>();
					for (int i =0; i<rowLabels.size(); i++) propArray.add(propMatrix[i]);
					BVhash.put(propKey+"/"+prop+Constants.CALC_MATRIX, propArray);
				}
			}
		}
		catch(RuntimeException e){
			logger.warn("BVhash not filled");
		}
	}

	/**
	 * Clears the tempSelectHash if anything is inside of it and fills the BV hashtable accordingly.
	 */
	public void run() {
		tempSelectHash.clear();
		try {
			FillWithSelect();
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}

	}

	private void errorCheck(String key, String rowLabel, String colLabel, Double val){
		if(key.equals("System/TError") && (val<0 || val>1 )){
			fileLogger.info("Illegal System - TError value: " + rowLabel + " " + colLabel + " " + val);
			logger.info("Illegal System - TError value: " + rowLabel + " " + colLabel + " " + val);
		}
	}
}
