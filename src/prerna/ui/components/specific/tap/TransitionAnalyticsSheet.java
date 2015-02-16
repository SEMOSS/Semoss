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

import java.awt.Color;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Hashtable;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextArea;

import prerna.ui.components.BrowserGraphPanel;

/**
 * Sheet used to show Transition analytics (3 charts breaking down costs)
 */
public class TransitionAnalyticsSheet extends JPanel {

	JLabel sysNoLabel, dataNoLabel, icdNoLabel;
	JTextArea transAllSysArea, transAllDataArea, transAllICDArea, transAllWSPArea,transAllWSCArea;
	Hashtable inputValues = new Hashtable();
	double[] soaCost;
	double icdCost;
	
	/**
	 * Constructor for TransitionAnalyticsSheet.
	 * 
	 * @param inputValues Hashtable	List of values needed to create the charts
	 */
	public TransitionAnalyticsSheet(Hashtable inputValues)
	{
		this.inputValues=inputValues;

		createSheet();
	}

	/**
	 * Sets sheet layout and adds Swing components
	 */
	public void createSheet()
	{
		setVisible(true);

		setBackground(Color.WHITE);
		BrowserGraphPanel tab1 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab1.setPreferredSize(new Dimension(500, 400));

		BrowserGraphPanel tab2 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab2.setPreferredSize(new Dimension(500, 400));

		BrowserGraphPanel tab3 = new BrowserGraphPanel("/html/MHS-RDFSemossCharts/app/singlechart.html");
		tab3.setPreferredSize(new Dimension(500, 400));
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[]{0, 0, 0};
		gridBagLayout.rowHeights = new int[]{0, 0, 0};
		gridBagLayout.columnWeights = new double[]{0.0, 0.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{0.0, 0.0, Double.MIN_VALUE};
		setLayout(gridBagLayout);

		GridBagConstraints gbc_panel_1_1 = new GridBagConstraints();
		gbc_panel_1_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1_1.fill = GridBagConstraints.BOTH;
		gbc_panel_1_1.gridx = 0;
		gbc_panel_1_1.gridy = 0;
		add(tab1, gbc_panel_1_1);

		GridBagConstraints gbc_panel_2 = new GridBagConstraints();
		gbc_panel_2.insets = new Insets(0, 0, 5, 0);
		gbc_panel_2.fill = GridBagConstraints.BOTH;
		gbc_panel_2.gridx = 1;
		gbc_panel_2.gridy = 0;
		add(tab2, gbc_panel_2);

		GridBagConstraints gbc_panel = new GridBagConstraints();
		gbc_panel.insets = new Insets(0, 0, 0, 5);
		gbc_panel.fill = GridBagConstraints.BOTH;
		gbc_panel.gridx = 0;
		gbc_panel.gridy = 1;
		add(tab3,  gbc_panel);

		Hashtable chartHash1 = createChart1();
		Hashtable chartHash2 = createChart2();
		Hashtable chartHash3 = createChart3();
		tab1.callIt(chartHash1);
		tab2.callIt(chartHash2);
		tab3.callIt(chartHash3);
	}

	/**
	 * Sets hashtable values for Cost Breakdown Per Year chart 1
	 * 
	 * @return Hashtable	List of values needed to create the chart
	 */
	private Hashtable createChart1()
	{
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("title",  "Cost Breakdown Per Year Graph #1");
		barChartHash.put("yAxisTitle", "Cost");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		String[] years = {"2014", "2015", "2016", "2017", "2018", "2019", "2020"};
		double[] icdCost = (double[]) inputValues.get("icdMainCost");
		double[] icdSOACost = (double[]) inputValues.get("soaICDMainCost");
		double[] soaCost = (double[]) inputValues.get("soaBuildCost");
		barChartHash.put("xAxis", years);
		seriesHash.put("Current As-Is ICD Maintenance Cost", icdCost);
		seriesHash.put("ICD Maintenance Cost in Transition to SOA", icdSOACost);
		seriesHash.put("SOA Build and Maintenance Cost", soaCost);
		colorHash.put("Current As-Is ICD Maintenance Cost", "#4572A7");
		colorHash.put("ICD Maintenance Cost in Transition to SOA", "#80699B");
		colorHash.put("SOA Build and Maintenance Cost", "#3D96AE");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
	}

	/**
	 * Sets hashtable values for Cost Breakdown Per Year chart 2
	 * 
	 * @return Hashtable	List of values needed to create the chart
	 */
	private Hashtable createChart2()
	{
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "column");
		barChartHash.put("title",  "Cost Breakdown Per Year Graph #2");
		barChartHash.put("yAxisTitle", "Cost");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		String[] years = {"2014", "2015", "2016", "2017", "2018", "2019", "2020"};
		double[] icdCost = (double[]) inputValues.get("icdMainCost");
		double[] icdSOACost = (double[]) inputValues.get("soaICDMainCost");
		double[] soaCost = (double[]) inputValues.get("soaBuildCost");
		double[] totalCost = new double[7];
		for (int i=0; i<icdCost.length;i++)
		{
			totalCost[i]=icdSOACost[i]+soaCost[i];
		}
		barChartHash.put("xAxis", years);
		seriesHash.put("Current As-Is ICD Maintenance Cost", icdCost);
		seriesHash.put("Total Cost for Transition to SOA", totalCost);
		colorHash.put("Current As-Is ICD Maintenance Cost", "#4572A7");
		colorHash.put("Total Cost for Transition to SOA", "#B5CA92");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
	}

	/**
	 * Sets hashtable values for Cumulative Cost chart
	 * 
	 * @return Hashtable	List of values needed to create the chart
	 */
	private Hashtable createChart3()
	{
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "line");
		barChartHash.put("title",  "Cumulative Cost over Years");
		barChartHash.put("yAxisTitle", "Cumulative Cost");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		String[] years = {"2014", "2015", "2016", "2017", "2018", "2019", "2020"};
		double[] icdCost = (double[]) inputValues.get("icdMainCost");
		double[] icdSOACost = (double[]) inputValues.get("soaICDMainCost");
		double[] soaCost = (double[]) inputValues.get("soaBuildCost");
		double[] totalCost = new double[7];
		double[] cumuICDCost = new double[7];
		cumuICDCost[0] = icdCost[0];
		double[] cumuSOACost = new double[7];

		for (int i=0; i<icdCost.length;i++)
		{
			totalCost[i]=icdSOACost[i]+soaCost[i];
		}
		cumuSOACost[0] = totalCost[0];
		for (int i= 1; i<cumuICDCost.length;i++)
		{
			cumuICDCost[i]=cumuICDCost[i-1]+icdCost[i];
			cumuSOACost[i]=cumuSOACost[i-1]+totalCost[i];
		}
		barChartHash.put("xAxis", years);
		seriesHash.put("Current As-Is ICD Maintenance Cost", cumuICDCost);
		seriesHash.put("Total Cost for Transition to SOA", cumuSOACost);
		colorHash.put("Current As-Is ICD Maintenance Cost", "#4572A7");
		colorHash.put("Total Cost for Transition to SOA", "#B5CA92");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
	}
}
