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
package prerna.algorithm.impl.specific.tap;


import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.ArrayList;

import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JTextArea;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.InputPanelPlaySheet;

/**
 * This class is used to optimize the calculations for univariate services.
 */
public class UnivariateOpt implements IAlgorithm{
	
	static final Logger logger = LogManager.getLogger(UnivariateOpt.class.getName());
	
	InputPanelPlaySheet playSheet;
	public int maxYears;
	double interfaceCost;
	public double serMainPerc;
	int noOfPts;
	double minBudget;
	double maxBudget;
	double hourlyCost;
	double[] learningConstants;
	public double iniLC;
	public int scdLT;
	public double scdLC;
	double attRate;
	double hireRate;
	public double infRate;
	double disRate;
	public UnivariateOptFunction f;
	double optBudget =0.0;
	JProgressBar progressBar;
	JTextArea consoleArea;
	
	/**
	 * Method setVariables.
	 * @param maxYears int
	 * @param interfaceCost double
	 * @param serMainPerc double
	 * @param attRate double
	 * @param hireRate double
	 * @param infRate double
	 * @param disRate double
	 * @param noOfPts int
	 * @param minBudget double
	 * @param maxBudget double
	 * @param hourlyCost double
	 * @param iniLC double
	 * @param scdLT int
	 * @param scdLC double
	 */
	public void setVariables(int maxYears, double interfaceCost, double serMainPerc, double attRate, double hireRate, double infRate, double disRate, int noOfPts, double minBudget, double maxBudget, double hourlyCost, double iniLC, int scdLT, double scdLC)
	{
		this.maxYears = maxYears;
		this.interfaceCost = interfaceCost*1000000;
		this.serMainPerc = serMainPerc;
		this.noOfPts = noOfPts;
		this.minBudget = minBudget*1000000;
		this.maxBudget = maxBudget*1000000;
		this.hourlyCost = hourlyCost;
		this.attRate = attRate;
		this.hireRate = hireRate;
		this.iniLC = iniLC;
		this.scdLT = scdLT;
		this.scdLC = scdLC;
		this.infRate = infRate;
		this.disRate = disRate;
	}

	public void setProgressBar(JProgressBar progressBar) {
		this.progressBar = progressBar;
	}
	
	public void setConsoleArea(JTextArea consoleArea) {
		this.consoleArea = consoleArea;
	}

	/**
	 * Runs the optimization for services.
	 */
	public void optimize()
	{
        
	}
	
	public void createTabAndDisplayList(String[] colNames,ArrayList <Object []> list, String panelTitle, Boolean addScroll) {
		JPanel panel;
		int tabIndex = playSheet.tabbedPane.indexOfTab(panelTitle);
		if(tabIndex < 0) {
			panel = new JPanel();
			playSheet.tabbedPane.addTab(panelTitle, null, panel, null);
			GridBagLayout gbl_specificSysAlysPanel = new GridBagLayout();
			gbl_specificSysAlysPanel.columnWidths = new int[] { 0, 0 };
			gbl_specificSysAlysPanel.rowHeights = new int[] { 0, 0 };
			gbl_specificSysAlysPanel.columnWeights = new double[] { 1.0, Double.MIN_VALUE };
			gbl_specificSysAlysPanel.rowWeights = new double[] { 1.0, Double.MIN_VALUE };
			panel.setLayout(gbl_specificSysAlysPanel);
		}else
			panel = (JPanel)playSheet.tabbedPane.getComponentAt(tabIndex);
		
		displayListOnTab(colNames,list,panel, addScroll);

	}
	
	public void displayListOnTab(String[] colNames,ArrayList <Object []> list,JPanel panel) {
		displayListOnTab(colNames, list, panel, false);
	}
		

	public void displayListOnTab(String[] colNames,ArrayList <Object []> list,JPanel panel, Boolean addScroll) {
		GridScrollPane pane = new GridScrollPane(colNames, list);
		if(addScroll)
			pane.addHorizontalScroll();
		panel.removeAll();
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gridBagLayout);
		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		panel.add(pane, gbc_panel_1_1);
		panel.repaint();
	}
	
	/**
	 * Clears the playsheet by removing information from all panels.
	 */
	public void clearPlaysheet(){
		playSheet.clearLabels();
		playSheet.setGraphsVisible(false);
		playSheet.clearPanels();
	}
	
	/**
	 * Sets the passed playsheet as a service optimization playsheet.
	 * @param 	playSheet	Playsheet to be cast.
	 */
	@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (InputPanelPlaySheet) playSheet;
		
	}

	/**
	 * Gets variable names.
	
	 * //TODO: Return empty object instead of null
	 * @return String[] */
	@Override
	public String[] getVariables() {
		return null;
	}

	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute(){
		optimize();
		
	}

	/**
	 * Gets the name of the algorithm.
	
	 * //TODO: Return empty object instead of null
	 * @return 	Algorithm name. */
	@Override
	public String getAlgoName() {
		return null;
	}
}
