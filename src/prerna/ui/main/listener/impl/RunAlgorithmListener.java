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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JProgressBar;
import javax.swing.JTabbedPane;
import javax.swing.JToggleButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.components.playsheets.ClassifyClusterPlaySheet;
import prerna.ui.components.playsheets.ClusteringVizPlaySheet;
import prerna.ui.components.playsheets.WekaClassificationPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Utility;

/**
 * Runs the algorithm selected on the Cluster/Classify playsheet and adds additional tabs. Tied to the button to the ClassifyClusterPlaySheet.
 */
public class RunAlgorithmListener extends AbstractListener {
	private static final Logger LOGGER = LogManager.getLogger(RunAlgorithmListener.class.getName());
	
	private ClassifyClusterPlaySheet playSheet;
	private JTabbedPane jTab;
	private JProgressBar jBar;
	private Hashtable<String, IPlaySheet> playSheetHash;
	private JComboBox<String> algorithmComboBox;
	private JComboBox<String> classificationMethodComboBox;
	private JComboBox<String> classComboBox;
	private JToggleButton showDrillDownBtn;
	private JComboBox<String> drillDownTabSelectorComboBox;
	private ArrayList<JCheckBox> columnCheckboxes;
	private String[] names;
	private ArrayList<Object[]> list;
	private IEngine engine;
	
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		//get the columns we're filtering out
		//create a new tab
		//figure out if we are clustering or classifying
		//if cluster.... TODO drilldown eventually
		//.............. pass tab, and call the clustering on it
		//if classifying... figure out the class and the method for classifying, run the classifying
		//need to put chart/grid on correct tabs, updating if necessary
		//TODO add logger statements
		
		//filter the names array and list of data based on the independent variables selected
		//must make sure that we include the first col, even though there is no checkbox
		boolean[] includeColArr = new boolean[columnCheckboxes.size()+1];
		includeColArr[0] = true; //this is the "title" or "name"
		for(int i=0;i<columnCheckboxes.size();i++) {
			JCheckBox checkbox = columnCheckboxes.get(i);
			includeColArr[i+1] = checkbox.isSelected();
		}
		String[] filteredNames = Utility.filterNames(names, includeColArr);
		ArrayList<Object[]> filteredList = Utility.filterList(list,includeColArr);

		BrowserPlaySheet newPlaySheet;
		String algorithm = algorithmComboBox.getSelectedItem() + "";
		if(algorithm.equals("Cluster") ) {
			showDrillDownBtn.setVisible(true);
			newPlaySheet = new ClusteringVizPlaySheet();
			((ClusteringVizPlaySheet)newPlaySheet).setAddAsTab(true);
			((ClusteringVizPlaySheet)newPlaySheet).drillDownData(names,filteredNames,list,filteredList);
			((ClusteringVizPlaySheet)newPlaySheet).setDrillDownTabSelectorComboBox(drillDownTabSelectorComboBox);
			((ClusteringVizPlaySheet)newPlaySheet).setPlaySheetHash(playSheetHash);
		}
		else if(algorithm.equals("Classify")){
			//method of classification to use
			String classMethod = classificationMethodComboBox.getSelectedItem() + "";
			
			//determine the column index and name to classify on
			String classifier = classComboBox.getSelectedItem() + "";
			int classifierIndex = names.length-1;
			while(classifierIndex>-1&&!names[classifierIndex].equals(classifier)) {
				classifierIndex--;
			}
			
			if(classifierIndex<0){
				LOGGER.error("Cannot match classifier selected in drop down to list of classifiers");
				return;
			}
			
			newPlaySheet = new WekaClassificationPlaySheet();
			newPlaySheet.setList(filteredList);
			newPlaySheet.setNames(filteredNames);
			((WekaClassificationPlaySheet)newPlaySheet).setModelName(classMethod);
			((WekaClassificationPlaySheet)newPlaySheet).setClassColumn(classifierIndex);

		} else {
			LOGGER.error("Cannot find algorithm");
			return;
		}
		newPlaySheet.setJTab(jTab);
		newPlaySheet.setJBar(jBar);
		newPlaySheet.setRDFEngine(engine);
		
		PlaysheetCreateRunner runner = new PlaysheetCreateRunner(newPlaySheet);
		Thread playThread = new Thread(runner);
		playThread.start();
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		this.playSheet = (ClassifyClusterPlaySheet)view;
		this.algorithmComboBox = playSheet.getAlgorithmComboBox();
		this.classificationMethodComboBox = playSheet.getClassificationMethodComboBox();
		this.classComboBox = playSheet.getClassComboBox();
		this.columnCheckboxes = playSheet.getColumnCheckboxes();
		this.names = playSheet.getNames();
		this.list = playSheet.getList();
		this.jTab = playSheet.getJTab();
		this.jBar = playSheet.getJBar();
		this.engine = playSheet.engine;
		this.drillDownTabSelectorComboBox = playSheet.getDrillDownTabSelectorComboBox();
		this.showDrillDownBtn = playSheet.getShowDrillDownBtn();
		this.playSheetHash = playSheet.getPlaySheetHash();
	}

}
