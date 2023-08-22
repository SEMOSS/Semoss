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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.swing.JButton;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableCellRenderer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.om.InsightStore;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.GridTableRowSorter;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class SystemPropertyGridPlaySheet extends GridPlaySheet {
	
	private static final Logger logger = LogManager.getLogger(SystemPropertyGridPlaySheet.class.getName());
	private String costQuery = "SELECT DISTINCT ?System (SUM(?FY15) as ?fy15) (SUM(?FY16) as ?fy16) (SUM(?FY17) as ?fy17) (SUM(?FY18) as ?fy18) (SUM(?FY19) as ?fy19) WHERE { SELECT DISTINCT ?System ?GLTag ?FY15 ?FY16 ?FY17 ?FY18 ?FY19 WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System> ;} {?SystemBudgetGLItem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemBudgetGLItem> ;} {?System <http://semoss.org/ontologies/Relation/Has> ?SystemBudgetGLItem} {?GLTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/GLTag> ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/TaggedBy> ?GLTag} OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY15 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY15>} } OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY16 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY16>} } OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY17 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY17>} } OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY18 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY18>} } OPTIONAL { {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/Contains/Cost> ?FY19 ;} {?SystemBudgetGLItem <http://semoss.org/ontologies/Relation/OccursIn> <http://health.mil/ontologies/Concept/FYTag/FY19>} } } } GROUP BY ?System BINDINGS ?GLTag {(<http://health.mil/ontologies/Concept/GLTag/Grand_Total>)}";
	private String tapPortfolio = "TAP_Portfolio";
	private String tapCost = "TAP_Cost_Data";
	
	private String[] varNames;
	private String[] costDataVarNames;
	private Integer costDataLength = 0;
	
	private boolean useAccountingFormat = true;
	
	private String[] names;
	private ArrayList<Object[]> list;
	
	public void setAccountingFormat(boolean useAccountingFormat) {
		this.useAccountingFormat = useAccountingFormat;
	}
	
	@Override
	public void createData() {
		Hashtable<String, Hashtable<String, Double>> costHash = new Hashtable<String, Hashtable<String, Double>>();
		
		boolean includeCost = true;
		// Create engines to be queried on
		IDatabaseEngine portfolioEngine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(tapPortfolio);
		IDatabaseEngine costEngine = (IDatabaseEngine) DIHelper.getInstance().getLocalProp(tapCost);
		
		if (portfolioEngine != null) { // process cost query from portfolio (faster on smaller db)
			costHash = processCostQuery(portfolioEngine);
		} else if (costEngine != null) {
			costHash = processCostQuery(costEngine);
		} else {
			includeCost = false;
		}
		
		ISelectWrapper runQuery = WrapperManager.getInstance().getSWrapper(engine, query);
		varNames = runQuery.getVariables();
		
		this.names = new String[varNames.length + costDataLength];
		for (int i = 0; i < names.length; i++) {
			if (i < varNames.length) {
				names[i] = varNames[i];
			} else {
				names[i] = costDataVarNames[i - varNames.length + 1];
			}
		}
		
		list = new ArrayList<Object[]>();
		while (runQuery.hasNext()) {
			Object[] addRow = new Object[varNames.length + costDataLength];
			
			ISelectStatement sjss = runQuery.next();
			String sysName = "";
			for (int i = 0; i < varNames.length + 1; i++) {
				if (i == 0) {
					sysName = sjss.getVar(varNames[i]).toString();
					addRow[i] = sysName;
				} else if (i == varNames.length) {
					if (includeCost) {
						Hashtable<String, Double> costInfo = costHash.get(sysName);
						if (costInfo != null) {
							for (int j = 0; j < costDataLength; j++) {
								if (costInfo.get(costDataVarNames[j + 1]) != null) {
									if (useAccountingFormat) {
										DecimalFormat nf = new DecimalFormat("\u00A4 #,##0.00");
										addRow[i + j] = nf.format(Math.round(costInfo.get(costDataVarNames[j + 1])));
									} else
										addRow[i + j] = Math.round(costInfo.get(costDataVarNames[j + 1]));
								}
							}
						} else {
							for (int j = 0; j < costDataLength; j++) {
								addRow[i + j] = "No cost info received.";
							}
						}
					}
				} else {
					addRow[i] = sjss.getVar(varNames[i]);
				}
			}
			list.add(addRow);
		}
	}
	
	private Hashtable<String, Hashtable<String, Double>> processCostQuery(IDatabaseEngine engine) {
		Hashtable<String, Hashtable<String, Double>> costHash = new Hashtable<String, Hashtable<String, Double>>();
		
		ISelectWrapper costWrapper = WrapperManager.getInstance().getSWrapper(engine, costQuery);
		costDataVarNames = costWrapper.getVariables();
		costDataLength = costDataVarNames.length - 1;
		while (costWrapper.hasNext()) {
			ISelectStatement sjss = costWrapper.next();
			String sys = sjss.getVar(costDataVarNames[0]).toString();
			Hashtable<String, Double> innerHash = new Hashtable<String, Double>();
			costHash.put(sys, innerHash);
			for (int i = 1; i < costDataVarNames.length; i++) {
				if (sjss.getVar(costDataVarNames[i]) != null && !sjss.getVar(costDataVarNames[i]).toString().equals("")) {
					innerHash.put(costDataVarNames[i], (Double) sjss.getVar(costDataVarNames[i]));
				}
			}
		}
		
		return costHash;
	}
	
	/**
	 * Sets the string version of the SPARQL query on the playsheet.
	 * @param query
	 */
	@Override
	public void setQuery(String query) {
		if (query.startsWith("SELECT") || query.startsWith("CONSTRUCT"))
			this.query = query;
		else {
			logger.info("Query " + query);
			
			String addConditions = "";
			String addBindings = "";
			
			int semicolon1 = query.indexOf(";");
			int semicolon2 = query.indexOf(" SELECT", semicolon1 + 1);
			
			String dispositionUserResponse = query.substring(0, semicolon1);
			String archiveUserResponse = query.substring(semicolon1 + 1, semicolon2);
			query = query.substring(semicolon2 + 1);
			
			//if all, don't need to add any conditions
			if (dispositionUserResponse.equals("High")) {
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'High'} {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))");
			} else if (dispositionUserResponse.equals("LPI")) {
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPI'}  {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))");
			} else if (dispositionUserResponse.equals("LPNI")) {
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Disposition> 'LPNI'}  {?System <http://semoss.org/ontologies/Relation/Contains/Device> 'N'} {?System <http://semoss.org/ontologies/Relation/Contains/Review_Status> ?Review_Status}FILTER (?Review_Status in('FAC_Approved','FCLG_Approved'))");
			}
			
			if (archiveUserResponse.equals("Yes")) {
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> 'Y' } ");
			} else if (archiveUserResponse.equals("No")) {
				addConditions = addConditions.concat("{?System <http://semoss.org/ontologies/Relation/Contains/Archive_Req> 'N' } ");
			}
			
			String retString = "";
			retString = retString.concat(query.substring(0, query.lastIndexOf("WHERE {"))).concat("WHERE {").concat(addConditions).concat(
					query.substring(query.lastIndexOf("WHERE {") + 8)).concat(" ").concat(addBindings);
			logger.info("New Query " + retString);
			this.query = retString;
		}
	}
	
	@Override
	public List<Object[]> getList() {
		return this.list;
	}
	
	@Override 
	public String[] getNames() {
		return this.names;
	};
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public void createView() {
		if (list.isEmpty()) {
			String questionID = getQuestionID();
			InsightStore.getInstance().remove(questionID);
			if (InsightStore.getInstance().isEmpty()) {
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;
		}
		super.createView();
		if (table == null)
			addPanel();
		
		updateProgressBar("80%...Creating Visualization", 80);
		gfd.setColumnNames(names);
		gfd.setDataList(list);
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setRowSorter(new GridTableRowSorter(model));
		
		DefaultTableCellRenderer rightRenderer = new DefaultTableCellRenderer();
		rightRenderer.setHorizontalAlignment(SwingConstants.RIGHT);
		
		for (int i = varNames.length; i < names.length; i++) {
			table.getColumnModel().getColumn(i).setCellRenderer(rightRenderer);
		}
		
		updateProgressBar("100%...Table Generation Complete", 100);
	}
}
