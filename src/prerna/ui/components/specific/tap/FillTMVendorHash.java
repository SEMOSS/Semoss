/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import javax.swing.JFrame;
import javax.swing.JList;
import javax.swing.JOptionPane;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This fills the hashtable for vendor specific technical maturity calculations.
 */
public class FillTMVendorHash extends GridPlaySheet{

	String query = null;
	IEngine engine = null;
	public Hashtable<String, Object> TMhash = new Hashtable<String, Object>();
	ISelectWrapper wrapper;
	ArrayList <String []> list;
	GridFilterData gfd = new GridFilterData();
	
	static final Logger logger = LogManager.getLogger(FillTMVendorHash.class.getName());
	
	//Necessary items to create a filler
	/**
	 * Sets the engine and query.
	 * @param query String
	 * @param engine IEngine
	 */
	public void setEngine (String query,
			IEngine engine) {
		this.query = query;
		this.engine = engine;
	}
	
	/**
	 * Creates the first view of any playsheet.
	 */
	@Override
	public void createView(){
	}
	/**
	 * Runs through all of the queries given pertaining to system software and hardware for the selected database.
	 * Calculate the technical maturity.
	 */
	public void runQueries()
	{
		//String MultipleQueries = this.sparql.getText();
		//first queries are create queries which count the rows to see how big of a matrix is required.  
		//Second queries are fill queries which fill the created matrices
		String MultipleQueries = "SELECT (\"CREATE\" AS ?CREATE_MATRIX) (\"VendorSoftware\" AS ?VendorSoftwareLifecycle) (\"NA\" AS ?type) (COUNT(DISTINCT(?ven)) AS ?sysCount) (COUNT(DISTINCT(?soft)) AS ?softCount) WHERE { {?ven <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;} {?uses <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?ven ?uses ?sys}{?uses2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?soft <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Software> ;} {?sys ?uses2 ?soft ;}{?cat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareType> ;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;} {?soft ?has ?cat} OPTIONAL{{?soft <http://semoss.org/ontologies/Relation/Contains/EOL> ?eol}}}"+"+++"+	
		"SELECT (\"CREATE\" AS ?CREATE_MATRIX) (\"VendorSoftware\" AS ?VendorSoftwareCategory) (\"NA\" AS ?type) (COUNT(DISTINCT(?ven)) AS ?sysCount) (COUNT(DISTINCT(?soft)) AS ?softCount) WHERE { {?ven <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;} {?uses <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?ven ?uses ?sys}{?uses2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?soft <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Software> ;} {?sys ?uses2 ?soft ;}{?cat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareType> ;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;} {?soft ?has ?cat} OPTIONAL{{?soft <http://semoss.org/ontologies/Relation/Contains/EOL> ?eol}}}"+"+++"+
		"SELECT DISTINCT (\"FILL\" AS ?FILL_MATRIX) (\"VendorSoftwareLifecycle\" AS ?VendorSoftwareLifecycle) (\"Lifecycle\" AS ?type) ?ven ?soft ?eol WHERE { {?ven <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;} {?uses <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?ven ?uses ?sys}{?uses2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?soft <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Software> ;} {?sys ?uses2 ?soft ;}{?cat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareType> ;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;} {?soft ?has ?cat} OPTIONAL{{?soft <http://semoss.org/ontologies/Relation/Contains/EOL> ?eol}}}"+"+++"+ 
		"SELECT DISTINCT (\"FILL\" AS ?FILL_MATRIX) (\"VendorSoftwareCategory\" AS ?VendorSoftwareCategory) (\"Category\" AS ?type) ?ven ?soft ?cat WHERE { {?ven <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;} {?uses <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?ven ?uses ?sys}{?uses2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?soft <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Software> ;} {?sys ?uses2 ?soft ;}{?cat <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SoftwareType> ;}{?has <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;} {?soft ?has ?cat} OPTIONAL{{?soft <http://semoss.org/ontologies/Relation/Contains/EOL> ?eol}}}"+"+++"+
		"SELECT DISTINCT (\"CREATE\" AS ?CREATE_MATRIX) (\"VendorHardware\" AS ?VendorHardware) (\"NA\" AS ?type) (COUNT(DISTINCT(?ven)) AS ?venCount) (COUNT(DISTINCT(?hard)) AS ?hardCount) WHERE { {?ven <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;} {?uses <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?ven ?uses ?sys}{?uses2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?hard <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Hardware> ;} {?sys ?uses2 ?hard ;}OPTIONAL{{?hard <http://semoss.org/ontologies/Relation/Contains/EOL> ?eol}}}"+"+++"+
		"SELECT DISTINCT (\"FILL\" AS ?FILL_MATRIX) (\"VendorHardware\" AS ?VendorHardware) (\"Hardware\" AS ?type) ?ven ?hard ?eol WHERE { {?ven <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Vendor>;} {?uses <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?ven ?uses ?sys}{?uses2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Uses> ;} {?hard <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Hardware> ;} {?sys ?uses2 ?hard ;}OPTIONAL{{?hard <http://semoss.org/ontologies/Relation/Contains/EOL> ?eol}}}";
		StringTokenizer queryTokens = new StringTokenizer(MultipleQueries, "+++");
		DIHelper.getInstance().setLocalProperty(Constants.TECH_MATURITY, TMhash);
		
		//set the engine to selected database
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repos = (Object [])list.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[0]+"");
		logger.info("Repository is " + repos);
		
		//run through all queries
		while(queryTokens.hasMoreElements()){
			String query = (String) queryTokens.nextElement();
			setEngine(query, (IEngine)engine);
			fillWithSelect();

		}

			TMVendorCalculationPerformer calculator = new TMVendorCalculationPerformer();
			TMhash = calculator.tmCalculate();
	}
	
	/**
	 * Popup window that appears when a query fails to run because the necessary information is unavailable in the RDF store.
	 * @param failedQuery String
	 */
	public void runFailPopup(String failedQuery){
		int loc = failedQuery.indexOf("WHERE");
		String queryIntro;
		if(loc>0) queryIntro = failedQuery.substring(0, loc);
		else queryIntro = "N/A";
		
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		JOptionPane.showMessageDialog(playPane, "The selected RDF store does not contain the necessary information to " +
			"perform the calculation.  Please select a different RDF store and try again. \n\n Failed query: \n"+queryIntro, 
			"Error", JOptionPane.ERROR_MESSAGE);
	}
	
	/**
	 * This retrieves the SELECT statement used to fill the TM hash.
	 * Executes the query in order to create the final TM hashtable.
	 */
	public void fillWithSelect() {
		
		if (DIHelper.getInstance().getLocalProp(Constants.TECH_MATURITY)!=null){
			TMhash = (Hashtable<String, Object>) DIHelper.getInstance().getLocalProp(Constants.TECH_MATURITY);
			logger.info("TMhash was previously filled");
		}
		logger.info("Filling TMhash " + query);
		
		wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		//wrapper = new SesameJenaSelectWrapper();
		list = new ArrayList();
	
		/*wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();*/
		
		try {
			processQuery();
			DIHelper.getInstance().setLocalProperty(Constants.TECH_MATURITY, TMhash);
		} catch (RuntimeException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Gets the list of names from the query.
	 * If it contains information about software lifecycle, software category, or system hardware, get the constants.
	 * Creates create or fill matrices with names if applicable.
	 */
	private void processQuery() {
		// get the bindings from it
		String [] names = wrapper.getVariables();
		
		String constant = null;
		if (names[1].contains("VendorSoftwareLifecycle")) constant = Constants.TM_LIFECYCLE;
		else if (names[1].contains("VendorSoftwareCategory")) constant = Constants.TM_CATEGORY;
		else if (names[1].contains("VendorHardware")) constant = Constants.TM_LIFECYCLE;
		

		if (names[0].contains("CREATE")){
				createMatrix(constant, names);
		}
		
		else if (names[0].contains("FILL")){
			if(names[0].contains("MATRIX")) fillStringMatrix(constant, names);
		}
	}
	
	/**
	 * Fills the string matrix with matrix name/type, row/column labels, and values.
	 * @param constant String
	 * @param names String[]
	 */
	public void fillStringMatrix(String constant, String[] names) {
		//This is for filling a matrix
		//first variable is FILL_MATRIX
		//second variable is matrix name
		//third variable is matrix type
		//fourth variable is row label
		//fifth variable is col label
		//sixth variable is value
		int count = 0;
		String var2 = null;
		String var3 = null;
		String var4 = null;
		String var5 = null;
		String key = null;
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				var2 = sjss.getVar(names[2]).toString();
				var3 = sjss.getVar(names[3]).toString();
				var4 = sjss.getVar(names[4]).toString();
				if(constant.contains("_Tech_Maturity_Lifecycle"))
					var5 = sjss.getRawVar(names[5]).toString();
				else
					var5 = sjss.getVar(names[5]).toString();

				if(count == 0){
					String vert3type = Utility.getClassName(sjss.getRawVar(names[3]).toString());
					String vert4type = Utility.getClassName(sjss.getRawVar(names[4]).toString());
					if (!(vert3type==null) && !(vert4type==null)) key = vert3type + "/" + vert4type;
					else key = var2;
				}
				count++;
				
				//get the right matrix and arrayList
				String[][] matrix = (String[][]) TMhash.get(key+constant);
				ArrayList<String> rowLabels = (ArrayList<String>) TMhash.get(key+Constants.CALC_ROW_LABELS);
				ArrayList<String> colLabels = (ArrayList<String>) TMhash.get(key+Constants.CALC_COLUMN_LABELS);
				
				//put in the label in rowLabels and put the value in the right spot in the array
				if (!rowLabels.contains(var3)) rowLabels.add(var3);
				int rowLoc = rowLabels.indexOf(var3);
				if (!colLabels.contains(var4)) colLabels.add(var4);
				int colLoc = colLabels.indexOf(var4);
				matrix[rowLoc][colLoc] = var5;
				TMhash.put(key+constant, matrix);
				TMhash.put(key+Constants.CALC_ROW_LABELS, rowLabels);
				TMhash.put(key+Constants.CALC_COLUMN_LABELS, colLabels);
			}
		}catch (RuntimeException e) {
			logger.fatal(e);
		}
	}
	
	/**
	 * Creates keys based on parameter names in the SesameJenaSelectWrapper.
	 * Puts the keys with appropriate values in the TM hash.
	 * @param constant String
	 * @param names String[]
	 */
	private void createMatrix(String constant, String [] names){
		try {
			while(wrapper.hasNext())
			{
				ISelectStatement sjss = wrapper.next();
				String key = null;

				if (names[1].contains("VendorSoftwareLifecycle")) {
						key = "Vendor/Software";
					}
					else if (names[1].contains("VendorSoftwareCategory")) {
						key = "Vendor/Software";
					}
					else if (names[1].contains("VendorHardware")) {
						key = "Vendor/Hardware";
					}
					Double vert3double = (Double) sjss.getVar(names[3]);
					int vert3int = (int) vert3double.intValue();
					Double vert4double = (Double) sjss.getVar(names[4]);
					int vert4int = (int) vert4double.intValue();
					
					String[][] fullMatrix = new String[vert3int][vert4int];
					ArrayList<String> rowLabels = new ArrayList<String>();
					ArrayList<String> colLabels = new ArrayList<String>();
					
					TMhash.put(key+constant, fullMatrix);
					TMhash.put(key+Constants.CALC_ROW_LABELS, rowLabels);
					TMhash.put(key+Constants.CALC_COLUMN_LABELS, colLabels);
				
			}
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}
	
	/**
	 * Fills the TM Vendor hash.
	 */
	@Override
	public void run() {
		createView();
	}	
}
