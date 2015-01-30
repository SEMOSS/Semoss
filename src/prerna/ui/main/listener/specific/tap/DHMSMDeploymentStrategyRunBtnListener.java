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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.specific.tap.DHMSMDeploymentStrategyPlaySheet;
import prerna.util.Utility;

/**
 */
public class DHMSMDeploymentStrategyRunBtnListener implements ActionListener {
	
	private static final Logger LOGGER = LogManager.getLogger(DHMSMDeploymentStrategyRunBtnListener.class.getName());

	private static final String YEAR_ERROR = "field is not an integer between 00 and 99, inclusive.";
	private static final String QUARTER_ERROR = "field is not an integer between 1 and 4, inclusive.";
	private static final String NON_INT_ERROR = "field contains a non-integer value.";
	private DHMSMDeploymentStrategyPlaySheet ps;
	private JTextArea consoleArea;
	
	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		LOGGER.info("Run Deployment Strategy Button Pushed");
		
		Hashtable<String, String> regionBeginHash = new Hashtable<String, String>();
		Hashtable<String, String> regionEndHash = new Hashtable<String, String>();
		
		consoleArea = ps.consoleArea;
		JToggleButton selectRegionTimesButton = ps.getSelectRegionTimesButton();
		if(!selectRegionTimesButton.isSelected()) {
			//pull from begin / end and fill the regions accordingly
			int beginQuarter = getInteger(ps.getBeginQuarterField(), ps.getBeginQuarterField().getName());
			int beginYear = getInteger(ps.getBeginYearField(), ps.getBeginYearField().getName());
			int endQuarter = getInteger(ps.getEndQuarterField(), ps.getEndQuarterField().getName());
			int endYear = getInteger(ps.getEndYearField(), ps.getEndYearField().getName());
			if(beginQuarter<0 || beginYear<0 || endQuarter<0 || endYear<0) {
				Utility.showError("Cannot read fields. Please check the Console tab for more information");
				return;
			}
			if(!validQuarter(beginQuarter, ps.getBeginQuarterField().getName()) || !validQuarter(endQuarter, ps.getEndQuarterField().getName()) || !validYear(beginYear, ps.getBeginYearField().getName())  || !validYear(endYear, ps.getEndYearField().getName()) ) {
				Utility.showError("Cannot read fields. Please check the Console tab for more information");
				return;
			}
				
			//do processing to fill the regionBegin and regionEndHash appropriately.
			
		}else {
			//pull from region list
			//check if region textfields are valid
			//add them to list of regions
			ArrayList<String> regionsList = ps.getRegionsList();
			ArrayList<JTextField> beginQuarterFieldRegionList = ps.getBeginQuarterFieldRegionList();
			ArrayList<JTextField> beginYearFieldRegionList = ps.getBeginYearFieldRegionList();
			ArrayList<JTextField> endQuarterFieldRegionList = ps.getEndQuarterFieldRegionList();
			ArrayList<JTextField> endYearFieldRegionList = ps.getEndYearFieldRegionList();

			for(int i=0;i<regionsList.size();i++) {
				int beginQuarter = getInteger(beginQuarterFieldRegionList.get(i), beginQuarterFieldRegionList.get(i).getName());
				int beginYear = getInteger(beginYearFieldRegionList.get(i), beginYearFieldRegionList.get(i).getName());
				int endQuarter = getInteger(endQuarterFieldRegionList.get(i), endQuarterFieldRegionList.get(i).getName());
				int endYear = getInteger(endYearFieldRegionList.get(i), endYearFieldRegionList.get(i).getName());
				if(beginQuarter<0 || beginYear<0 || endQuarter<0 || endYear<0) {
					Utility.showError("Cannot read fields. Please check the Console tab for more information");
					return;
				}
				if(!validQuarter(beginQuarter, beginQuarterFieldRegionList.get(i).getName()) || !validQuarter(endQuarter, endQuarterFieldRegionList.get(i).getName()) || !validYear(beginYear, endQuarterFieldRegionList.get(i).getName())  || !validYear(endYear, endYearFieldRegionList.get(i).getName()) ) {
					Utility.showError("Cannot read fields. Please check the Console tab for more information");
					return;
				}
				if(beginYear>=10)
					regionBeginHash.put(regionsList.get(i), "Q"+beginQuarter+"FY"+beginYear);
				else
					regionBeginHash.put(regionsList.get(i), "Q"+beginQuarter+"FY0"+beginYear);
				
				if(endYear>=10)
					regionEndHash.put(regionsList.get(i), "Q"+endQuarter+"FY"+endYear);
				else
					regionEndHash.put(regionsList.get(i), "Q"+endQuarter+"FY0"+endYear);

			}
		}
		
		//now have a filled region begin and end hash. run maher's code here
		for(String region : regionBeginHash.keySet()) {
			System.out.println("Region "+region);
			System.out.println("Begins "+regionBeginHash.get(region));
			System.out.println("Ends "+regionEndHash.get(region));
		}
			
	}
		
	/**
	 * Method setPlaySheet.
	 * @param sheet DHMSMDeploymentStrategyPlaySheet
	 */
	public void setPlaySheet(DHMSMDeploymentStrategyPlaySheet ps)
	{
		this.ps = ps;

	}
	
	/**
	 * Gets the integer in a textfield.
	 * if it does not contain an integer, throws an error.
	 * @param field
	 * @return
	 */
	private int getInteger(JTextField field, String fieldName) {
		String q = field.getText();
		try{
			int qInt = Integer.parseInt(q);
			return qInt;
		}catch (RuntimeException e) {
			consoleArea.setText(consoleArea.getText()+"\n"+fieldName+" "+NON_INT_ERROR);
			LOGGER.error(fieldName+" "+NON_INT_ERROR);
			return -1;
		}
	}
	
	/**
	 * Determines if the quarter is valid, between 1 and 4 inclusive
	 * @param quarter
	 * @param fieldName
	 * @return
	 */
	private Boolean validQuarter(int quarter, String fieldName) {
		if(quarter < 1 || quarter > 4) {
			consoleArea.setText(consoleArea.getText()+"\n"+fieldName+" "+QUARTER_ERROR);
			LOGGER.error(fieldName+" "+QUARTER_ERROR);
			return false;
		}
		return true;
	}
	
	/**
	 * Determines if the year is valid, between 0 and 99 inclusive
	 * @param year
	 * @param fieldName
	 * @return
	 */
	private Boolean validYear(int year, String fieldName) {
		if(year < 0 || year > 99) {
			consoleArea.setText(consoleArea.getText()+"\n"+fieldName+" "+YEAR_ERROR);
			LOGGER.error(fieldName+" "+YEAR_ERROR);
			return false;
		}
		return true;
	}

}