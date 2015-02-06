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
package prerna.ui.components.specific.cbp;

import java.awt.Dimension;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.model.BigdataLiteralImpl;

public class GBCDrillDownHeatTablePlaySheet extends BrowserPlaySheet{

	private static final Logger logger = LogManager.getLogger(GBCDrillDownHeatTablePlaySheet.class.getName());
	
	final String row1Name = "Functions";
	final String row2Name = "Process Categories";
	final String row3Name = "Lights-on IT";
	final String lightsOnIT = "_Lights-On_IT";
	final String lightsOnItKey = "lights-on_it";
	final String clientValueKey = "clientValueKey";
	final String peerValueKey = "peerValueKey";
	final String rowKey = "rowKey";
	final String colKey = "colKey";
	final String cellKey = "cellKey";
	//String[] colNames = new String[] {"http://semoss.org/ontologies/Concept/FunctionLevel1/Sales", "http://semoss.org/ontologies/Concept/FunctionLevel1/Marketing", "http://semoss.org/ontologies/Concept/FunctionLevel1/Product", "http://semoss.org/ontologies/Concept/FunctionLevel1/Distribution", "http://semoss.org/ontologies/Concept/FunctionLevel1/New_Business", "http://semoss.org/ontologies/Concept/FunctionCategoryLevel0/Customer_Service", "http://semoss.org/ontologies/Concept/FunctionCategoryLevel0/Corporate_Overhead", "http://semoss.org/ontologies/Concept/FunctionLevel1/Premium_Tax", "Adjusted_Line_of_Business", "Total_Line_of_Business"};
	String customTitle = "";
	Properties presProps = new Properties();
	
	ArrayList<String> passedQueries = new ArrayList<String>();
	
	public GBCDrillDownHeatTablePlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/columnchart.html";
		
		Properties props = (Properties) DIHelper.getInstance().getLocalProp("GBC_PROPERTIES");
		if(props == null)
		{
			String fileName = workingDir + "/GBCPresentationV2.properties";
			try {
				presProps.load(new FileInputStream(fileName));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
			presProps = props;
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash = new Hashtable<String, Hashtable<String, Hashtable<String, Object>>>(); // column header -> rowName -> value
		for( String passedQuery : passedQueries)
		{
			processingHash = processQuery(passedQuery, processingHash);
		}
		
		ArrayList<Hashtable<String, Hashtable<String, Object>>> dataObj = new ArrayList<Hashtable<String, Hashtable<String, Object>>>();
		ArrayList<Hashtable<String, Hashtable<String, Object>>> orderedArray = getOrderedArray(processingHash);
		dataObj.addAll(orderedArray);
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("title", this.customTitle);
		columnChartHash.put("dataSeries", dataObj);
		
		return columnChartHash;
	}
	
	private ArrayList<Hashtable<String, Hashtable<String, Object>>> getOrderedArray(Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash)
	{
		Object colList = presProps.get(engine.getEngineName());
		if(colList == null){
			colList = presProps.get(engine.getEngineName().replaceAll("_[vV]{1}[0-9]+$", ""));
		}
		if (colList!=null){ // if orders of the columns have been specified in the presentation file, use those, otherwise random order
			String colNames = colList + "";
			ArrayList<Hashtable<String, Hashtable<String, Object>>> orderedArray = new ArrayList<Hashtable<String, Hashtable<String, Object>>>();
			StringTokenizer tokens = new StringTokenizer(colNames, ";");
			int count = 0;
			while(tokens.hasMoreTokens()){
				String colName = tokens.nextToken();
				if(processingHash.containsKey(colName)){
					orderedArray.add(count, processingHash.get(colName));
					count++;
				}
			}
			return orderedArray;
		}
		else{
			ArrayList<Hashtable<String, Hashtable<String, Object>>> orderedArray = new ArrayList<Hashtable<String, Hashtable<String, Object>>>(processingHash.values());
			return orderedArray;
		}
	}
	
	private Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processQuery(String passedQuery, Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash)
	{
		SesameJenaSelectWrapper sjsw = Utility.processQuery(this.engine, passedQuery);
		String[] passedNames = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			
			String colHeader = getVariable(passedNames[0], sjss) + "";
			String cellName = getVariable(passedNames[1], sjss) + "";

			if(customTitle.isEmpty())
				customTitle = sjss.getVar(passedNames[2]) + " vs. " + sjss.getVar(passedNames[10]);
			
			Double clientVal = ((BigdataLiteralImpl)getVariable(passedNames[5], sjss)).doubleValue();
			Double peerVal = ((BigdataLiteralImpl)getVariable(passedNames[7], sjss)).doubleValue();
			Double denVal = ((BigdataLiteralImpl)getVariable(passedNames[9], sjss)).doubleValue();
			
			Double finalClientVal = (clientVal) * denVal;
			Double finalPeerVal = (peerVal) * denVal;
			
			String rowName = "";
			if(cellName.toLowerCase().contains(this.lightsOnItKey)){
				rowName = this.row3Name;
				
				//set cell name
				cellName = cellName.substring(0, cellName.lastIndexOf("/")+1)+Utility.getInstanceName(colHeader) + this.lightsOnIT;
			}
			else {
				rowName = this.row2Name;
			}
			
			addToRow(colHeader, processingHash, finalClientVal, finalPeerVal, rowName, cellName);
			addToRow(colHeader, processingHash, finalClientVal, finalPeerVal, this.row1Name, "");
				
			
		}
		
		return processingHash;
	}
	
	private void addToRow(String colHeader, Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash, Double clientVal, Double peerVal, String rowName, String cellName){
		Hashtable<String, Object> cellHash = new Hashtable<String, Object>();
		Hashtable<String, Hashtable<String, Object>> colHash = new Hashtable<String, Hashtable<String, Object>>();
		boolean exists = false;
		String colHashKey = Utility.getInstanceName(colHeader);
		if(processingHash.containsKey(colHashKey)){
			colHash = processingHash.get(colHashKey);
			if(colHash.containsKey(cellName)){
				cellHash = colHash.get(cellName);
				clientVal = clientVal + (Double) cellHash.get(clientValueKey);
				peerVal = peerVal + (Double) cellHash.get(peerValueKey);
				exists = true;
			}
		}
		if (!exists){
			colHash.put(cellName, cellHash);
			cellHash.put(this.rowKey, rowName);
			cellHash.put(this.colKey, colHeader);
			cellHash.put(this.cellKey, cellName);
		}
		cellHash.put(clientValueKey, clientVal);
		cellHash.put(peerValueKey, peerVal);
		processingHash.put(colHashKey, colHash);
		
	}

	@Override
	public void createData()
	{
		list = new ArrayList<Object[]>();
		list.add(new Object[0]);
		names = new String[0];
		dataHash = processQueryData();
	}

	@Override
	public void setQuery(String query) {
		String [] tokens = query.split("\\+\\+\\+");
		// format is mainQuery+++taxonomyQuery
		for (int queryIdx = 0; queryIdx < tokens.length; queryIdx++){
			String token = tokens[queryIdx];
			passedQueries.add(token);
			logger.info("Set passed query " + token);
		}
	}
	
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getRawVar(varName);
	}
	
}
