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
package prerna.ui.components.specific.tap;


import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
/**
 * This class fills the business value hashtable.
 */
public class FillCapabilityBVHash implements Runnable{

	String query = null;
	IEngine engine = null;
	Hashtable<String, Object> BVhash = new Hashtable<String, Object>();
	SesameJenaSelectWrapper wrapper;
	Hashtable tempSelectHash = new Hashtable();
	Logger logger = Logger.getLogger(getClass());
	
	//Necessary items to create a filler
	/**
	 * Constructor for FillBVHash.
	 * @param query String
	 * @param engine IEngine
	 */
	public FillCapabilityBVHash(String query,
			IEngine engine) {
		this.query = query;
		this.engine = engine;
	}
	
	/**
	 * This is the SELECT statement used to fill the BV hash.
	 * Executes the query in order to fill the appropriate arraylists used to create the final BV hashtable.
	 */
	public void FillWithSelect() throws Exception {
		if (DIHelper.getInstance().getLocalProp(Constants.CAPABILITY_BUSINESS_VALUE)!=null) 
			BVhash = (Hashtable<String, Object>) DIHelper.getInstance().getLocalProp(Constants.CAPABILITY_BUSINESS_VALUE);
		
		wrapper = new SesameJenaSelectWrapper();
	
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
			
		fillArrayLists();//fills arraylists with names and puts values in 5000x5000 matrix. Stores all in tempSelectHash
			
		try {
			fillSelectBVhash();//uses tempSelectHash to fill BVhash
			DIHelper.getInstance().setLocalProperty(Constants.CAPABILITY_BUSINESS_VALUE, BVhash);
			
		} catch (Exception e) {
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
	private void fillArrayLists() throws Exception {
		// get the bindings from it
		String [] names = wrapper.getVariables();
		int count = 0;
		//ArrayList<ArrayList<Double>> weightValues = new ArrayList<ArrayList<Double>>();
		double[][] matrix = new double[5000][5000];
		ArrayList<String> colLabels = new ArrayList<String>();
		ArrayList<String> rowLabels = new ArrayList<String>();
		
		String key = null;
		// now get the bindings and generate the data
		try {
			String[] variables = wrapper.getVariables();
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.BVnext();
				
				while(sjss==null && wrapper.hasNext()) {
					sjss=wrapper.BVnext();
				}
				//make key
				if(count == 0){
					
					String vert1uri = sjss.getVar(names[0])+"";
					String vert1type = Utility.getClassName(vert1uri);
					String vert2uri = sjss.getVar(names[2])+"";
					String vert2type = Utility.getClassName(vert2uri);
					key = vert1type + "/" + vert2type;
					String propOrRel = names[1]+"";
					if(propOrRel.equals("prop")){
						key=vert1type+"/"+propOrRel;
						colLabels.add(names[2]);
						colLabels.add(names[3]);
						colLabels.add(names[4]);
					}
				}
				count++;

				//if it is an edge triple, everything gets added
				String var0 = Utility.getInstanceName(sjss.getVar(names[0])+"");
				String var1 = Utility.getInstanceName(sjss.getVar(names[1])+"");
				String var2 = Utility.getInstanceName(sjss.getVar(names[2])+"");

				if(key.endsWith("prop"))
				{
					if (!rowLabels.contains(var0)){
						rowLabels.add(var0);
					}
					int rowLoc = rowLabels.indexOf(var0);
					matrix[rowLoc][0]=(Double) sjss.getVar(names[2]);
					matrix[rowLoc][1]=(Double) sjss.getVar(names[3]);
					matrix[rowLoc][2]=(Double) sjss.getVar(names[4]);
				}
				else if(var1.contains(var0)&&var1.contains(var2)){
					if (!rowLabels.contains(var0)){
						rowLabels.add(var0);
					}
					if (! colLabels.contains(var2))
						colLabels.add(var2);
					int rowLoc = rowLabels.indexOf(var0);
					int colLoc = colLabels.indexOf(var2);
					if(names.length<=3)
						matrix[rowLoc][colLoc] = 1.0;
					else if((sjss.getVar(names[3]) instanceof Double)){
						matrix[rowLoc][colLoc] = (Double) sjss.getVar(names[3]);
					}
				logger.debug("Creating new Value ");
				}
			}
		}catch (Exception e) {
			logger.fatal(e);
		}
		tempSelectHash.put(key+Constants.CALC_COLUMN_LABELS, colLabels);
		tempSelectHash.put(key+Constants.CALC_ROW_LABELS, rowLabels);
		tempSelectHash.put(key+Constants.CALC_MATRIX, matrix);
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
			for (int col = 0; col < matrix[0].length; col++){
				for(int row = 0; row<matrix.length; row++){
					matrix[row][col] = values[row][col];
					totalCheck = totalCheck + values[row][col];
				}
			}
			if(totalCheck == 0) logger.warn("Matrix filled with all zeros: " + key);
			BVhash.put(key+Constants.CALC_MATRIX, matrix);
			BVhash.put(key+Constants.CALC_COLUMN_LABELS, colLabels);
			BVhash.put(key+Constants.CALC_ROW_LABELS, rowLabels);
		}
		catch(Exception e){
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
		} catch (Exception e) {
			// TODO: Specify exception
			e.printStackTrace();
		}

	}
	

}
