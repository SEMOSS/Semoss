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

import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.UpdateProcessor;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
/**
 * This class performs the business value calculations.
 */
public class BVCalculationPerformer implements IAlgorithm,Runnable{

	public Hashtable<String, Object> BVhash = new Hashtable<String, Object>();
	
	static final Logger logger = LogManager.getLogger(BVCalculationPerformer.class.getName());
	GridFilterData gfd = new GridFilterData();
	String type;
	double soaAlphaValue;
	
	/**
	 * Constructor for BVCalculationPerformer.
	 */
	public BVCalculationPerformer() {
		
	}
	/**
	 * Sets the type.
	 * @param t 	Type, in string form.
	 */
	public void setType(String t){
		type = t;
	}
	/**
	 * Sets the SOA Alpha value.
	 * @param value 	Alpha value, in double form.
	 */
	public void setsoaAlphaValue(double value){
		soaAlphaValue = value;
	}

	/**
	 * Processes the query used to run the calculation for business value.
	 */
	public void runCalculation() {
		logger.info("Graph PlaySheet Begining Calculation");
		Hashtable paramHash = new Hashtable();
		if(!type.equals("vendor"))
		{
			double networkBetaValue = 1-soaAlphaValue;
			paramHash.put("soaAlphaValue", Double.toString(soaAlphaValue));
			paramHash.put("networkBetaValue", Double.toString(networkBetaValue));
		}
		try {
			fillHash(paramHash);
			if (DIHelper.getInstance().getLocalProp(Constants.BUSINESS_VALUE)!=null) 
				BVhash = (Hashtable<String, Object>) DIHelper.getInstance().getLocalProp(Constants.BUSINESS_VALUE);
			System.out.println(BVhash.toString());
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


			//SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data");

			String query = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;}}";
			ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
			
//			wrapper.setQuery(query);
//			wrapper.setEngine(engine);
//			wrapper.executeQuery();
			String[] names = wrapper.getVariables();

			while(wrapper.hasNext())
			{
				ISelectStatement sjss= wrapper.next();
				String system = (String)sjss.getVar(names[0]);
				if(!businessNames.contains(system))
				{
					businessNames.add(system);
					businessValues.add(0.0);
				}

			}

			String businessQuery = prepareInsert(businessNames, businessValues, "BusinessValue");
			
			UpdateProcessor pro = new UpdateProcessor();
			pro.setQuery(businessQuery);
			pro.processQuery();
			
		} catch (RuntimeException e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}

	/**
	 * Obtains the SOA alpha value and network beta values if these are not for vendors.
	 * Runs the business value calculations.
	 */
	public void run() {
		logger.info("Graph PlaySheet Begining Calculation");
		Hashtable paramHash = new Hashtable();
		if(!type.equals("vendor"))
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
	 * Runs the appropriate query on the set engine to calculate business value.
	 * @param paramHash Hashtable of parameters.
	 */
	private void fillHash(Hashtable paramHash){
		//String MultipleQueries = this.sparql.getText();
		String MultipleQueries;
		if(type.equals("System"))
			//SOA Alpha Value vs. Network Beta Value is in the query themselves
			MultipleQueries = "SELECT ?activity1 ?need ?data ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?activity1 ?need ?data ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?system ?provide ?data (?weight2*@soaAlphaValue@ AS ?Weight2) WHERE{ {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide  <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?system ?provide ?data ;} {?provide <http://semoss.org/ontologies/Relation/Contains/weight>  ?weight2}}+++SELECT ?system ?provide ?data (?NETWORKweight*@networkBetaValue@ AS ?NETWORKWeight) WHERE{ {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide  <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?system ?provide ?data ;} {?provide <http://semoss.org/ontologies/Relation/Contains/NetworkWeight>  ?NETWORKweight}}+++SELECT ?activity1 ?need ?blu ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?activity1 ?need ?blu ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?system ?provide ?blu ?weight2 WHERE { {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?system <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?provide  <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?system ?provide ?blu ;} {?provide <http://semoss.org/ontologies/Relation/Contains/weight> ?weight2}}+++SELECT ?act ?have (IF(?rel=<http://semoss.org/ontologies/Relation/Contains/Dataweight>,<http://semoss.org/ontologies/Concept/Element/Data>,<http://semoss.org/ontologies/Concept/Element/BLU>) AS ?element) ?weight1 WHERE {{?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?act ?rel ?weight1} }BINDINGS ?rel { (<http://semoss.org/ontologies/Relation/Contains/Dataweight>)(<http://semoss.org/ontologies/Relation/Contains/BLUweight>)}+++SELECT ?sys (\"pred\" AS ?pred) (IF(BOUND(?avail), \"Concept/TError/1-Availability-Actual\", ?has) AS ?obj) (COALESCE(1-?avail, ?sum) AS ?total) WHERE {{SELECT ?sys (SAMPLE(?have) AS ?has) (SAMPLE(?terror) AS ?error) (SUM(?weight1) AS ?sum) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;} {?terror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError> ;} {?sys ?have ?terror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}} GROUP BY ?sys}OPTIONAL{ {?sys <http://semoss.org/ontologies/Relation/Contains/Availability-Actual> ?avail} FILTER(datatype(?avail) = xsd:double) } }+++SELECT ?act ?have ?ferror ?weight1 WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned> ;} {?ferror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError> ;} {?act ?have ?ferror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?bp ?consist ?act ?contains2 ?props WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>;} {?bp ?consist ?act ;} {?bp ?contains2 ?props}}+++SELECT ?bp ?consist ?act ?weight1 WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?bp ?consist ?act ;} {?consist <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}";
		else
			MultipleQueries = "SELECT ?activity1 ?need ?data ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?activity1 ?need ?data ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?service ?exposes ?data ?weight2 WHERE{ BIND(1.0 AS ?weight2){?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;} {?service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>;} {?exposes  <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?service ?exposes ?data ;} }+++SELECT ?activity1 ?need ?blu ?weight1 WHERE { {?activity1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?need <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs> ;} {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?activity1 ?need ?blu ;} {?need <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?service ?exposes ?blu ?weight2 WHERE{ BIND(1.0 AS ?weight2) {?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;} {?service <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service>;} {?exposes  <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Exposes>;} {?service ?exposes ?blu ;} }+++SELECT ?act ?have ?element ?weight1 WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/ActivitySplit> ;} {?element <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Element> ;} {?act ?have ?element ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?sys (\"pred\" AS ?pred) (IF(BOUND(?avail), \"Concept/TError/1-Availability-Actual\", ?has) AS ?obj) (COALESCE(1-?avail, ?sum) AS ?total) WHERE {{SELECT ?sys (SAMPLE(?have) AS ?has) (SAMPLE(?terror) AS ?error) (SUM(?weight1) AS ?sum) WHERE { {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Has> ;} {?terror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/TError> ;} {?sys ?have ?terror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}} GROUP BY ?sys}OPTIONAL{ {?sys <http://semoss.org/ontologies/Relation/Contains/Availability-Actual> ?avail} FILTER(datatype(?avail) = xsd:double) } }+++SELECT ?act ?have ?ferror ?weight1 WHERE { {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity>;} {?have <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Assigned> ;} {?ferror <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/FError> ;} {?act ?have ?ferror ;} {?have <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}+++SELECT ?bp ?consist ?act ?contains2 ?props WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>;} {?bp ?consist ?act ;} {?bp ?contains2 ?props}}+++SELECT ?bp ?consist ?act ?weight1 WHERE { {?bp <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess>;} {?consist <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists> ;} {?act <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;} {?bp ?consist ?act ;} {?consist <http://semoss.org/ontologies/Relation/Contains/weight> ?weight1}}";
		
		StringTokenizer queryTokens = new StringTokenizer(MultipleQueries, "+++");
		while(queryTokens.hasMoreElements()){
			String unfilledQuery = (String) queryTokens.nextElement();
			//need to fill the query (if it has @@) for the alpha and beta values
			String query = Utility.fillParam(unfilledQuery, paramHash);
			
			// now just do class.forName for this layout Value and set it inside playsheet
			// need to template this out and there has to be a directive to identify 
			// specifically what sheet we need to refer to
			
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			Object[] repos = (Object [])list.getSelectedValues();
			IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repos[0]+"");
			logger.info("Repository is " + repos);
			FillBVHash filler = new FillBVHash(query, (IEngine)engine);
			filler.run();
			
		}
	}
	
	/**
	 * Used to test printing results to an Excel workbook.
	 * @param key 	Key used in the filename and to get information from the business value hashtable.
	 */
//	private void testPrint(String key){
//		FileOutputStream fileOut = null;
//		try {
//			fileOut = new FileOutputStream(key +"_testPrint.xls");
//			HSSFWorkbook workbook = new HSSFWorkbook();
//			HSSFSheet worksheet = workbook.createSheet("Data");
//			ArrayList<Double> values = (ArrayList<Double>) BVhash.get(key+Constants.CALC_MATRIX);
//			ArrayList<String> names = (ArrayList<String>) BVhash.get(key+Constants.CALC_NAMES_LIST);
//			for (int row=0; row<names.size(); row++){
//				HSSFRow row1 = worksheet.createRow((short) row);
//				HSSFCell cell = row1.createCell(0);
//				cell.setCellValue((String) names.get(row));
//				HSSFCell cell1 = row1.createCell(1);
//				cell1.setCellValue((double) values.get(row));
//			}
//		workbook.write(fileOut);
//		fileOut.flush();
//		fileOut.close();
//
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}finally{
//			try{
//				if(fileOut!=null)
//					fileOut.close();
//			}catch(IOException e) {
//				e.printStackTrace();
//			}
//		}
//		logger.info("Printed: " +key +" testPrint.xls");
//	}	
	
	/**
	 * Prints the matrix into an Excel workbook.
	 * @param keyPart1 	First part of the key for the filename and obtaining values from the BV hash.
	 * @param keyPart2 	Second part of the key for the filename and obtaining values from the BV hash.
	 */
//	private void printMatrix(String keyPart1, String keyPart2){
//		//mprint(activitySystem);
//		String key = keyPart1+"/"+keyPart2;
//		FileOutputStream fileOut = null;
//		try {
//			fileOut = new FileOutputStream(keyPart1.replaceAll("/", "") +"-"+keyPart2 +"_Matrix.xls");
//			HSSFWorkbook workbook = new HSSFWorkbook();
//			HSSFSheet worksheet = workbook.createSheet("Data");
//			double[][] matrix = (double[][]) BVhash.get(key+Constants.CALC_MATRIX);
//			ArrayList<String> rowNames = (ArrayList<String>) BVhash.get(key+Constants.CALC_ROW_LABELS);
//			ArrayList<String> colNames = (ArrayList<String>) BVhash.get(key+Constants.CALC_COLUMN_LABELS);
//			HSSFRow row0 = worksheet.createRow((short) 0);
//			for (int colTitle = 1; colTitle<colNames.size()+1; colTitle++){
//				HSSFCell cell = row0.createCell(colTitle);
//				cell.setCellValue((String) colNames.get(colTitle-1));
//			}
//			for (int row=1; row<rowNames.size()+1; row++){
//				HSSFRow row1 = worksheet.createRow((short) row);
//				HSSFCell cell = row1.createCell(0);
//				cell.setCellValue((String) rowNames.get(row-1));
//				for(int col = 1; col<colNames.size()+1; col++){
//					HSSFCell cell1 = row1.createCell(col);
//					cell1.setCellValue((double) matrix[row-1][col-1]);
//				}
//			}
//		workbook.write(fileOut);
//		fileOut.flush();
//
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}finally{
//			try{
//				if(fileOut!=null)
//					fileOut.close();
//			}catch(IOException e) {
//				e.printStackTrace();
//			}
//		}
//		logger.info("Printed: " + keyPart1+"-"+keyPart2 +"_Matrix.xls");
//	}	
	
//	/**
//	 * Prints the matrix into an Excel workbook.
//	 * @param keyPart1 	First part of the key for the filename and obtaining values from the BV hash.
//	 * @param keyPart2 	Second part of the key for the filename and obtaining values from the BV hash.
//	 */
//	private void printMatrix(String keyPart1, String keyPart2){
//		//mprint(activitySystem);
//		String key = keyPart1+"/"+keyPart2;
//		try {
//			FileOutputStream fileOut = new FileOutputStream(keyPart1.replaceAll("/", "") +"-"+keyPart2 +"_Matrix.xls");
//			HSSFWorkbook workbook = new HSSFWorkbook();
//			HSSFSheet worksheet = workbook.createSheet("Data");
//			double[][] matrix = (double[][]) BVhash.get(key+Constants.CALC_MATRIX);
//			ArrayList<String> rowNames = (ArrayList<String>) BVhash.get(key+Constants.CALC_ROW_LABELS);
//			ArrayList<String> colNames = (ArrayList<String>) BVhash.get(key+Constants.CALC_COLUMN_LABELS);
//			HSSFRow row0 = worksheet.createRow((short) 0);
//			for (int colTitle = 1; colTitle<colNames.size()+1; colTitle++){
//				HSSFCell cell = row0.createCell(colTitle);
//				cell.setCellValue((String) colNames.get(colTitle-1));
//			}
//			for (int row=1; row<rowNames.size()+1; row++){
//				HSSFRow row1 = worksheet.createRow((short) row);
//				HSSFCell cell = row1.createCell(0);
//				cell.setCellValue((String) rowNames.get(row-1));
//				for(int col = 1; col<colNames.size()+1; col++){
//					HSSFCell cell1 = row1.createCell(col);
//					cell1.setCellValue((double) matrix[row-1][col-1]);
//				}
//			}
//		workbook.write(fileOut);
//		fileOut.flush();
//		fileOut.close();
//
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		logger.info("Printed: " + keyPart1+"-"+keyPart2 +"_Matrix.xls");
//	}	
	
	/**
	 * Transposes a matrix.
	 * @param matrixKey 	Key of the first matrix.
	
	 * @return double[][]	Transposed matrix. */
	public double[][] transposeMatrix(String matrixKey){
		logger.info("Transposing " + matrixKey);
		double[][] matrix = (double[][]) BVhash.get(matrixKey+Constants.CALC_MATRIX);
		ArrayList<String> m1rowLabels = (ArrayList<String>) BVhash.get(matrixKey+Constants.CALC_ROW_LABELS);
		ArrayList<String> m1colLabels = (ArrayList<String>) BVhash.get(matrixKey+Constants.CALC_COLUMN_LABELS);
		double[][] newMatrix = new double[matrix[0].length][matrix.length];
		for (int row = 0; row<matrix.length; row++){
			for (int col = 0; col<matrix[0].length; col++){
				newMatrix[col][row] = matrix[row][col];
			}
		}

		StringTokenizer matrixTokens = new StringTokenizer(matrixKey, "/");
		String first = (String) matrixTokens.nextElement();
		String second = (String) matrixTokens.nextElement();
		String newKey = second+"/"+first;
	    BVhash.put(newKey+Constants.CALC_MATRIX, newMatrix);
	    BVhash.put(newKey+Constants.CALC_COLUMN_LABELS, m1rowLabels);
	    BVhash.put(newKey+Constants.CALC_ROW_LABELS, m1colLabels);
	    
		logger.info("Transposed: " + matrixKey);
		return newMatrix;
	}
	
	/**
	 * Calculates business value by multiplying the results from the business process activities and the adjusted
	 * activity type error matrices. This information is stored in the business value hash.
	
	 * @return ArrayList<Double>	Final business value calculations. */
	public ArrayList<Double> calculateBusinessValue(){
		double[][] processSystem = multiply("BPasis/Final", type+"ActivityTypeErrAdj/Final");
		ArrayList<Double> result = new ArrayList<Double>();
		for (int i = 0; i<processSystem[0].length;i++){
			double total=0;
			for (int j =0; j<processSystem.length; j++){
				total = total + processSystem[j][i];
			}
			result.add(total);
		}
		BVhash.put(type+Constants.BUSINESS_VALUE+Constants.CALC_MATRIX, result);
		BVhash.put(type+Constants.BUSINESS_VALUE+Constants.CALC_NAMES_LIST, BVhash.get("BPasis/"+type+"ActivityTypeErrAdj/Final"+Constants.CALC_COLUMN_LABELS));
		logger.info("Calculated BV");
		return result;
	}
	
	/**
	 * Calculates the activity/IO split and activity/BLU split and uses this information to calculate
	 * the adjusted activity type error.
	
	 * @return double[][] 	Contains final calculations. */
	public double[][] calculateActivitySystemErrAdj(){
		if(type.equalsIgnoreCase("system")) addMatrix("System/DataObjectNETWORK", "System/DataObject");
		transposeMatrix(type+"/DataObject");
		double[][] actSysIO = multiply("Activity/DataObject", "DataObject/"+type);
		transposeMatrix(type+"/BusinessLogicUnit");
		double[][] actSysBLU = multiply("Activity/BusinessLogicUnit", "BusinessLogicUnit/"+type);
		double[][] activitySplit = (double[][]) BVhash.get("Activity/Element"+Constants.CALC_MATRIX);
		int dataLoc = ((ArrayList<String>) BVhash.get("Activity/Element"+Constants.CALC_COLUMN_LABELS)).indexOf("Data");
		int bluLoc = ((ArrayList<String>) BVhash.get("Activity/Element"+Constants.CALC_COLUMN_LABELS)).indexOf("Business_Logic");
		if (bluLoc<0) bluLoc = ((ArrayList<String>) BVhash.get("Activity/Element"+Constants.CALC_COLUMN_LABELS)).indexOf("BLU");
		ArrayList<String> actSysBLUrowlabels = (ArrayList<String>) BVhash.get("Activity/BusinessLogicUnit/"+type+Constants.CALC_ROW_LABELS);
		double[][] actSplit = (double[][]) BVhash.get("Activity/Element_Matrix");
		double[][] actSysBLUSplit = new double[actSysBLU.length][actSysBLU[0].length];
		for (int i = 0; i<actSysBLU.length;i++){
			int actLoc = ((ArrayList<String>) BVhash.get("Activity/Element"+Constants.CALC_ROW_LABELS)).indexOf(actSysBLUrowlabels.get(i));
			if(actLoc>=0){
				for (int j =0; j<actSysBLU[0].length; j++){
					actSysBLUSplit[i][j]=actSysBLU[i][j]*actSplit[actLoc][bluLoc];
				}
			}
		}
		double[][] actSysIOSplit = new double[actSysIO.length][actSysIO[0].length];
		ArrayList<String> actSysIOrowlabels = (ArrayList<String>) BVhash.get("Activity/DataObject/"+type+Constants.CALC_ROW_LABELS);
		for (int i = 0; i<actSysIO.length;i++){
			int actLoc = ((ArrayList<String>) BVhash.get("Activity/Element_RowLabels")).indexOf(actSysIOrowlabels.get(i));
			if (actLoc>=0){
				for (int j =0; j<actSysIO[0].length; j++){
					actSysIOSplit[i][j]=actSysIO[i][j]*actSplit[actLoc][dataLoc];
				}
			}
		}
		BVhash.put("Activity/IO/Split/"+type+Constants.CALC_MATRIX, actSysIOSplit);
		BVhash.put("Activity/BLU/Split/"+type+Constants.CALC_MATRIX, actSysBLUSplit);
		BVhash.put("Activity/IO/Split/"+type+Constants.CALC_ROW_LABELS, actSysIOrowlabels);
		BVhash.put("Activity/BLU/Split/"+type+Constants.CALC_ROW_LABELS, actSysBLUrowlabels);
		BVhash.put("Activity/IO/Split/"+type+"_ColLabels", BVhash.get("Activity/DataObject/"+type+Constants.CALC_COLUMN_LABELS));
		BVhash.put("Activity/BLU/Split/"+type+"_ColLabels", BVhash.get("Activity/BusinessLogicUnit/"+type+Constants.CALC_COLUMN_LABELS));
		
		double[][] actSys = addMatrix("Activity/IO/Split/"+type, "Activity/BLU/Split/"+type);
		
		ArrayList actSysCol = (ArrayList) BVhash.get("Activity/"+type+Constants.CALC_COLUMN_LABELS);
		ArrayList actSysRow = (ArrayList) BVhash.get("Activity/"+type+Constants.CALC_ROW_LABELS);
		
		ArrayList<String> actSysColLabels = (ArrayList<String>) BVhash.get("Activity/"+type+Constants.CALC_COLUMN_LABELS);
		ArrayList<String> sysTErrorRowLabels = (ArrayList<String>) BVhash.get(type+"/TError"+Constants.CALC_ROW_LABELS);
		double[][] result = new double[actSys.length][actSys[0].length];
		double[][] sysTError = (double[][]) BVhash.get(type+"/TError"+Constants.CALC_MATRIX);
		//first need the sum for each system
		for (int i = 0; i<actSys[0].length;i++){
			double sum = 0;
			if(sysTErrorRowLabels!=null){
				int index= sysTErrorRowLabels.indexOf(actSysColLabels.get(i));
				if(index>=0){
					for(int col = 0; col<sysTError[0].length; col++)sum = sum + sysTError[index][col];
				}
			}
			for (int j =0; j<actSys.length; j++){
				result[j][i]=actSys[j][i]*(1-sum);
			}
		}
		BVhash.put(type+"ActivityTypeErrAdj/Final"+Constants.CALC_MATRIX, result);
		BVhash.put(type+"ActivityTypeErrAdj/Final"+Constants.CALC_COLUMN_LABELS, actSysCol);
		BVhash.put(type+"ActivityTypeErrAdj/Final"+Constants.CALC_ROW_LABELS, actSysRow);
		
		logger.info("Calculated "+type+"ActivityTypeErrAdj/Final");
		return result;
	}
	
	/**
	 * Given information about various business processes, such as efficiency, initial effectiveness, initial 
	 * efficiency, wartime criticality, number of transations, and errors, this method calculates the the 
	 * value of these various business processes and puts them in the BV hash.
	
	 * @return double[][] 	Contains final calculations. */
	public double[][] calculateBPasis(){
		ArrayList<String> BPnames = (ArrayList<String>) BVhash.get("BusinessProcess"+Constants.CALC_NAMES_LIST);
		ArrayList<Double> BPefficiency = (ArrayList<Double>) BVhash.get("BusinessProcess/Efficiency" + Constants.CALC_MATRIX);
		ArrayList<Double> BPinitialEffectiveness = (ArrayList<Double>) BVhash.get("BusinessProcess/Initial_Effectiveness" + Constants.CALC_MATRIX);
		ArrayList<Double> BPinitialEfficiency = (ArrayList<Double>) BVhash.get("BusinessProcess/Initial_Efficiency" + Constants.CALC_MATRIX);
		ArrayList<Double> BPwartimeCriticality = (ArrayList<Double>) BVhash.get("BusinessProcess/Wartime_Criticality" + Constants.CALC_MATRIX);
		ArrayList<Double> BPtransactionsNum = (ArrayList<Double>) BVhash.get("BusinessProcess/Transactions_Num" + Constants.CALC_MATRIX);
		double[][] activityAError = (double[][]) BVhash.get("Activity/FError" + Constants.CALC_MATRIX);
		double[] activityAErrorColSum = sumColumns(activityAError);
		ArrayList<String> activityAErrorRowLabels = (ArrayList<String>) BVhash.get("Activity/FError"+Constants.CALC_ROW_LABELS);
		double[][] BPactivity = (double[][]) BVhash.get("BusinessProcess/Activity"+Constants.CALC_MATRIX);
		ArrayList<String> BPactivityRowLabels = (ArrayList<String>) BVhash.get("BusinessProcess/Activity"+Constants.CALC_ROW_LABELS);
		ArrayList<String> BPactivityColLabels = (ArrayList<String>) BVhash.get("BusinessProcess/Activity"+Constants.CALC_COLUMN_LABELS);
		double sumProductWarTrans = 0;
		for (int i = 0; i < BPnames.size(); i++){
			if (!(BPtransactionsNum.get(i)==0.0)){
				sumProductWarTrans = BPwartimeCriticality.get(i)*Math.log(BPtransactionsNum.get(i)) + sumProductWarTrans;
			}
		}
		double[][] result = new double[BPactivity.length][BPactivity[0].length];
		for (int i = 0; i<BPactivity.length;i++){
			int bpLoc = BPnames.indexOf(BPactivityRowLabels.get(i));
			for (int j =0; j<BPactivity[0].length; j++){
				int actLoc = activityAErrorRowLabels.indexOf(BPactivityColLabels.get(j));
				if(actLoc>=0 && bpLoc>=0){
					if(BPtransactionsNum.get(bpLoc)==0.0){
						result[i][j]=((1-activityAErrorColSum[actLoc])*BPefficiency.get(bpLoc)-(BPinitialEfficiency.get(bpLoc)*BPinitialEffectiveness.get(bpLoc)))*
								((BPwartimeCriticality.get(bpLoc)*(BPtransactionsNum.get(bpLoc)))/(sumProductWarTrans))*
								BPactivity[i][j];
					}
					else{
						result[i][j]=((1-activityAErrorColSum[actLoc])*BPefficiency.get(bpLoc)-(BPinitialEfficiency.get(bpLoc)*BPinitialEffectiveness.get(bpLoc)))*
								((BPwartimeCriticality.get(bpLoc)*Math.log(BPtransactionsNum.get(bpLoc)))/(sumProductWarTrans))*
								BPactivity[i][j];
					}
				}
				if(actLoc>=0&&bpLoc<0){
					result[i][j]=0.0;
				}
				if(actLoc<0 && bpLoc>=0){
					if(BPtransactionsNum.get(bpLoc)==0.0){
						result[i][j]=((1)*BPefficiency.get(bpLoc)-(BPinitialEfficiency.get(bpLoc)*BPinitialEffectiveness.get(bpLoc)))*
								((BPwartimeCriticality.get(bpLoc)*(BPtransactionsNum.get(bpLoc)))/(sumProductWarTrans))*
								BPactivity[i][j];
					}
					else{
						result[i][j]=((1)*BPefficiency.get(bpLoc)-(BPinitialEfficiency.get(bpLoc)*BPinitialEffectiveness.get(bpLoc)))*
								((BPwartimeCriticality.get(bpLoc)*Math.log(BPtransactionsNum.get(bpLoc)))/(sumProductWarTrans))*
								BPactivity[i][j];
					}
				}
			}
		}
		BVhash.put("BPasis/Final"+Constants.CALC_MATRIX, result);
		BVhash.put("BPasis/Final"+Constants.CALC_COLUMN_LABELS, BPactivityColLabels);
		BVhash.put("BPasis/Final"+Constants.CALC_ROW_LABELS, BPactivityRowLabels);
		logger.info("Calculated BPasis");
		return result;
	}
	
	/**
	 * Sum the columns for each row in the matrix.
	 * @param matrix 	Matrix containing columns to be summed.
	
	 * @return double[] 	New column sums. */
	public double[] sumColumns(double [][] matrix){
		double[] result = new double [matrix.length];
		for (int row = 0; row < matrix.length; row++){
			double columnSum = 0;
			for (int col = 0; col<matrix[0].length; col++){
				columnSum = columnSum + matrix[row][col];
			}
			result[row] = columnSum;
		}
		return result;
	}

	//this is going to take in the key name for the two different matrices
	//it is going to require both of the matrices and all of the labels
	//matrix1 row labels become new labels.  matrix2 col labels become new labels
	//find which is shorter between matrix1 col labels and matrix2 row labels
	//the shorter will be the master
	//for each in the master, find the loc in the longer
	//make the multiplication

	/**
	 * Performs matrix multiplication necessary for business value calculations.
	 * Runs new calculations and returns the results. 
	 * Put the new matrix in the BV hashtable. This will have everything except the example edge.
	 * @param matrixKeyOne 		Key of the first matrix.
	 * @param matrixKeyTwo 		Key of the second matrix.
	
	 * @return double[][] 		Results from new calculations. */
	public double[][] multiply(String matrixKeyOne, String matrixKeyTwo) {
		double[][] m1 = (double[][]) BVhash.get(matrixKeyOne+Constants.CALC_MATRIX);
		double[][] m2 = (double[][]) BVhash.get(matrixKeyTwo+Constants.CALC_MATRIX);
		ArrayList<String> m1rowLabels = (ArrayList<String>) BVhash.get(matrixKeyOne+Constants.CALC_ROW_LABELS);
		ArrayList<String> m1colLabels = (ArrayList<String>) BVhash.get(matrixKeyOne+Constants.CALC_COLUMN_LABELS);
		ArrayList<String> m2rowLabels = (ArrayList<String>) BVhash.get(matrixKeyTwo+Constants.CALC_ROW_LABELS);
		ArrayList<String> m2colLabels = (ArrayList<String>) BVhash.get(matrixKeyTwo+Constants.CALC_COLUMN_LABELS);
		ArrayList<String> newrowLabels = new ArrayList<String>();
		newrowLabels.addAll(m1rowLabels);
		ArrayList<String> newcolLabels = new ArrayList<String>();
		newcolLabels.addAll(m2colLabels);
		ArrayList<String> shorterLabels = new ArrayList<String>();
		ArrayList<String> longerLabels = new ArrayList<String>();
		if(m1colLabels.size()>m2rowLabels.size()) {
			longerLabels.addAll(m1colLabels);
			shorterLabels.addAll(m2rowLabels);
		}
		else {
			longerLabels.addAll(m2rowLabels);
			shorterLabels.addAll(m1colLabels);
		}

	    //make the new key to put in BVhash
		StringTokenizer m1Tokens = new StringTokenizer(matrixKeyOne, "/");
		StringTokenizer m2Tokens = new StringTokenizer(matrixKeyTwo, "/");
		String newKey = m1Tokens.nextElement()+"/"+m2Tokens.nextElement()+"/"+m2Tokens.nextElement();
		
		int m1rows = m1rowLabels.size();
//		int longer = longerLabels.size();
		int shorter = shorterLabels.size();
	    int m2cols = m2colLabels.size();
	    double[][] result = new double[m1rows][m2cols];
	    for (int i=0; i<m1rows; i++)
	      for (int j=0; j<m2cols; j++)
	        for (int k=0; k<shorter; k++){
	        	int m1loc = m1colLabels.indexOf(shorterLabels.get(k));
	        	int m2loc = m2rowLabels.indexOf(shorterLabels.get(k));
	        	if (m1loc<0 || m2loc<0) result[i][j] += 0;
	        	else result[i][j] += m1[i][m1loc] * m2[m2loc][j];
	        	if(Double.isNaN(result[i][j])){
	        		logger.warn("Encountered NaN in matrix: " + newKey);
	        	}
	        }
	    BVhash.put(newKey+Constants.CALC_MATRIX, result);
	    BVhash.put(newKey+Constants.CALC_COLUMN_LABELS, newcolLabels);
	    BVhash.put(newKey+Constants.CALC_ROW_LABELS, newrowLabels);
	    logger.info("Multiplied: " + matrixKeyOne +" with " + matrixKeyTwo);
	    return result;
	    
	  }


	//take in the key of the two matrices to add
	//new col labels becomes whichever is longer
	//new row labels becomes whichever is longer
	//add and put in BVhash
	//new key is first element of first key and last element of last key
	/**
	 * Gets original calculation matrices from the business value hashtable.
	 * Runs new calculations and returns the results.
	 * @param matrixKeyOne 		Key of the first matrix.
	 * @param matrixKeyTwo 		Key of the second matrix.
	
	 * @return double[][] 		Results from new calculations. */
	public double[][] addMatrix(String matrixKeyOne, String matrixKeyTwo) {
		double[][] m1 = (double[][]) BVhash.get(matrixKeyOne+Constants.CALC_MATRIX);
		double[][] m2 = (double[][]) BVhash.get(matrixKeyTwo+Constants.CALC_MATRIX);
		if(m1==null) return m2;
		if(m2==null) return m1;
		ArrayList<String> m1rowLabels = (ArrayList<String>) BVhash.get(matrixKeyOne+Constants.CALC_ROW_LABELS);
		ArrayList<String> m1colLabels = (ArrayList<String>) BVhash.get(matrixKeyOne+Constants.CALC_COLUMN_LABELS);
		ArrayList<String> m2rowLabels = (ArrayList<String>) BVhash.get(matrixKeyTwo+Constants.CALC_ROW_LABELS);
		ArrayList<String> m2colLabels = (ArrayList<String>) BVhash.get(matrixKeyTwo+Constants.CALC_COLUMN_LABELS);
		ArrayList<String> newrowLabels = new ArrayList<String>();
		ArrayList<String> newcolLabels = new ArrayList<String>();
		
		newcolLabels.addAll(m1colLabels);
		for(String m2label : m2colLabels){
			if(!newcolLabels.contains(m2label)) newcolLabels.add(m2label);
		}
		newrowLabels.addAll(m1rowLabels);
		for(String m2label : m2rowLabels){
			if(!newrowLabels.contains(m2label)) newrowLabels.add(m2label);
		}
		int rows = newrowLabels.size();
		int col = newcolLabels.size();
		double[][] result = new double[rows][col];
		
		StringTokenizer m1Tokens = new StringTokenizer(matrixKeyOne, "/");
		StringTokenizer m2Tokens = new StringTokenizer(matrixKeyTwo, "/");
		int m2size = m2Tokens.countTokens();
		for(int i = 0; i<m2size-1; i++) m2Tokens.nextElement();
		String newKey = m1Tokens.nextElement()+"/"+m2Tokens.nextElement();
		
		double m1total = 0;
		double m2total = 0;
		//handles uneven sizes by saying if an item doesn't exist it is equal to 0
		//keeps total for the matrix to throw warning if either is all zeros.
		for (int i = 0; i < rows; i++){
			for (int j = 0; j < col; j++){
				int m1rowLoc = m1rowLabels.indexOf(newrowLabels.get(i));
				int m2rowLoc = m2rowLabels.indexOf(newrowLabels.get(i));
				int m1colLoc = m1colLabels.indexOf(newcolLabels.get(j));
				int m2colLoc = m2colLabels.indexOf(newcolLabels.get(j));
				if(m1rowLoc >= 0 && m1colLoc>= 0 && m2rowLoc>=0 && m2colLoc>=0){
					result[i][j] = m1[m1rowLoc][m1colLoc] + m2[m2rowLoc][m2colLoc];
					m1total = m1total + m1[m1rowLoc][m1colLoc];
					m2total = m2total + m2[m2rowLoc][m2colLoc];
				}
				else if ((m1rowLoc<0 || m1colLoc<0)&&(m2rowLoc>=0 && m2colLoc>=0)){
					result[i][j] = m2[m2rowLoc][m2colLoc];
					m2total = m2total + m2[m2rowLoc][m2colLoc];
				}
				else if ((m2rowLoc<0 || m2colLoc<0)&&(m1rowLoc>=0 && m1colLoc>=0)){
					result[i][j] = m1[m1rowLoc][m1colLoc];
					m1total = m1total + m1[m1rowLoc][m1colLoc];
				}

	        	if(Double.isNaN(result[i][j])){
	        		logger.warn("Encountered NaN in matrix " + newKey);
	        	}
			}
		}
		if(m1total == 0) logger.warn("The following matrix is all zeros: " + matrixKeyOne);
		if(m2total == 0) logger.warn("The following matrix is all zeros: " + matrixKeyTwo);

	    BVhash.put(newKey+Constants.CALC_MATRIX, result);
	    BVhash.put(newKey+Constants.CALC_COLUMN_LABELS, newcolLabels);
	    BVhash.put(newKey+Constants.CALC_ROW_LABELS, newrowLabels);
	    logger.info("Added: " + matrixKeyOne +" with " + matrixKeyTwo);
	    
		return result;  
	}

	/**
	 * Given a set of parameters, creates the query to be inserted.
	 * @param names 	List of system names.
	 * @param values 	List of business values associated with systems.
	 * @param propName 	Property name string used to create the predicate (relation) URI.
	
	 * @return String 	Query to be inserted. */
	public String prepareInsert(ArrayList<String> names, ArrayList<Double> values, String propName){
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
			if(type.equals("Vendor"))
				subjectUri = "<http://health.mil/ontologies/Concept/Vendor/"+names.get(sysIdx) +">";
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
