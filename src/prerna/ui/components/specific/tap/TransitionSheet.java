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

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JTextArea;

import prerna.ui.components.NewHoriScrollBarUI;
import prerna.ui.components.NewScrollBarUI;

/**
 * Transition report sheet used to display systems, data, and ICDs affected.
 */
public class TransitionSheet extends JPanel {

	JLabel sysNoLabel, dataNoLabel, icdNoLabel, genDataLbl, genBLULbl, dataFedLbl, dataConLbl, bluProLbl;
	JLabel genDataCostLbl, genBLUCostLbl, dataFedCostLbl, dataConCostLbl, bluProCostLbl;
	JTextArea transAllSysArea, transAllDataArea, transAllICDArea, transAllGenDataArea,transAllGenBLUArea, transAllDataFedArea, transAllDataConArea, transAllBLUProArea;
	Hashtable inputValues = new Hashtable();
	/*
	public TransitionSheet (Hashtable inputValues)
	{
		this.inputValues = inputValues;
	}
	@Override
	public void run() {
		this.runAnalysis();
	}
	*/
	/**
	 * Constructor for TransitionSheet.
	 * 
	 * @param inputValues Hashtable
	 */
	public TransitionSheet(Hashtable inputValues)
	{
		this.inputValues=inputValues;
		createSheet();
		setValues();
		//addToFrame();
	}

	/**
	 * Sets return values.
	 * 
	 * @param retValues Hashtable of values to be set as return values
	 */
	public void setRetValues(Hashtable retValues)
	{
		this.inputValues = retValues;
	}
	
	/**
	 * Sets up Swing components for sheet.
	 */
	public void createSheet()
	{
		
		//new JInternalFrame("Service-Oriented Architecture Analysis");
		//setIconifiable(true);
		//setClosable(true);
		//setMaximizable(true);

		//setBounds(6, 157, 684, 490);
		GridBagLayout gridBagLayout_2 = new GridBagLayout();
		gridBagLayout_2.columnWidths = new int[]{0, 0, 0, 0};
		gridBagLayout_2.rowHeights = new int[]{0, 0, 0, 0, 0, 0};
		gridBagLayout_2.columnWeights = new double[]{0.0, 0.0, 0.0, Double.MIN_VALUE};
		gridBagLayout_2.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.setLayout(gridBagLayout_2);
		
		JPanel panel = new JPanel();
		GridBagConstraints gbc_panel = new GridBagConstraints();
		gbc_panel.anchor = GridBagConstraints.NORTHWEST;
		gbc_panel.insets = new Insets(0, 0, 5, 5);
		gbc_panel.gridx = 0;
		gbc_panel.gridy = 0;
		//this.getContentPane().add(panel, gbc_panel);
		this.add(panel, gbc_panel);
		GridBagLayout gbl_panel = new GridBagLayout();
		gbl_panel.columnWidths = new int[]{186, 0};
		gbl_panel.rowHeights = new int[]{0, 0, 0, 0};
		gbl_panel.columnWeights = new double[]{0.0, Double.MIN_VALUE};
		gbl_panel.rowWeights = new double[]{0.0, 0.0, 0.0, Double.MIN_VALUE};
		panel.setLayout(gbl_panel);
		
		JLabel lblSystemAnalysis = new JLabel("System Analysis");
		GridBagConstraints gbc_lblSystemAnalysis = new GridBagConstraints();
		gbc_lblSystemAnalysis.insets = new Insets(0, 0, 5, 0);
		gbc_lblSystemAnalysis.anchor = GridBagConstraints.NORTHWEST;
		gbc_lblSystemAnalysis.gridx = 0;
		gbc_lblSystemAnalysis.gridy = 0;
		panel.add(lblSystemAnalysis, gbc_lblSystemAnalysis);
		
		sysNoLabel = new JLabel("Number of Systems Affected:");
		GridBagConstraints gbc_sysNoLabel = new GridBagConstraints();
		gbc_sysNoLabel.insets = new Insets(0, 0, 5, 0);
		gbc_sysNoLabel.anchor = GridBagConstraints.NORTHWEST;
		gbc_sysNoLabel.gridx = 0;
		gbc_sysNoLabel.gridy = 1;
		panel.add(sysNoLabel, gbc_sysNoLabel);
		
		transAllSysArea = new JTextArea();
		transAllSysArea.setEditable(false);
		JScrollPane transAllSysAreaScroll = new JScrollPane (transAllSysArea);
		transAllSysAreaScroll.getVerticalScrollBar().setUI(new NewScrollBarUI());
		transAllSysAreaScroll.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		transAllSysAreaScroll.setMinimumSize(new Dimension(250, 250));
		transAllSysAreaScroll.setPreferredSize(new Dimension(250,250));
		GridBagConstraints gbc_transAllSysAreaScroll = new GridBagConstraints();
		gbc_transAllSysAreaScroll.anchor = GridBagConstraints.WEST;
		gbc_transAllSysAreaScroll.fill = GridBagConstraints.VERTICAL;
		gbc_transAllSysAreaScroll.gridx = 0;
		gbc_transAllSysAreaScroll.gridy = 2;
		panel.add(transAllSysAreaScroll, gbc_transAllSysAreaScroll);
		
		JPanel panel_1 = new JPanel();
		GridBagConstraints gbc_panel_1 = new GridBagConstraints();
		gbc_panel_1.anchor = GridBagConstraints.WEST;
		gbc_panel_1.insets = new Insets(0, 0, 5, 5);
		gbc_panel_1.fill = GridBagConstraints.VERTICAL;
		gbc_panel_1.gridx = 1;
		gbc_panel_1.gridy = 0;
		//this.getContentPane().add(panel_1, gbc_panel_1);
		this.add(panel_1, gbc_panel_1);
		GridBagLayout gbl_panel_1 = new GridBagLayout();
		gbl_panel_1.columnWidths = new int[]{0, 0};
		gbl_panel_1.rowHeights = new int[]{0, 0, 0, 0};
		gbl_panel_1.columnWeights = new double[]{0.0, Double.MIN_VALUE};
		gbl_panel_1.rowWeights = new double[]{0.0, 0.0, 0.0, Double.MIN_VALUE};
		panel_1.setLayout(gbl_panel_1);
		
		JLabel lblDataAnalysis = new JLabel("Data Analysis");
		GridBagConstraints gbc_lblDataAnalysis = new GridBagConstraints();
		gbc_lblDataAnalysis.insets = new Insets(0, 0, 5, 0);
		gbc_lblDataAnalysis.anchor = GridBagConstraints.WEST;
		gbc_lblDataAnalysis.gridx = 0;
		gbc_lblDataAnalysis.gridy = 0;
		panel_1.add(lblDataAnalysis, gbc_lblDataAnalysis);
		
		dataNoLabel = new JLabel("Number of Data Objects Affected:");
		GridBagConstraints gbc_dataNoLabel = new GridBagConstraints();
		gbc_dataNoLabel.insets = new Insets(0, 0, 5, 0);
		gbc_dataNoLabel.anchor = GridBagConstraints.WEST;
		gbc_dataNoLabel.gridx = 0;
		gbc_dataNoLabel.gridy = 1;
		panel_1.add(dataNoLabel, gbc_dataNoLabel);
		
		transAllDataArea = new JTextArea();
		transAllDataArea.setEditable(false);
		JScrollPane transAllDataAreaScroll = new JScrollPane (transAllDataArea);
		transAllDataAreaScroll.getVerticalScrollBar().setUI(new NewScrollBarUI());
		transAllDataAreaScroll.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		transAllDataAreaScroll.setPreferredSize(new Dimension(250,250));
		transAllDataAreaScroll.setMinimumSize(new Dimension(250,250));
		GridBagConstraints gbc_transAllDataAreaScroll = new GridBagConstraints();
		gbc_transAllDataAreaScroll.fill = GridBagConstraints.BOTH;
		gbc_transAllDataAreaScroll.gridx = 0;
		gbc_transAllDataAreaScroll.gridy = 2;
		panel_1.add(transAllDataAreaScroll, gbc_transAllDataAreaScroll);
		
		JPanel panel_2 = new JPanel();
		GridBagConstraints gbc_panel_2 = new GridBagConstraints();
		gbc_panel_2.anchor = GridBagConstraints.WEST;
		gbc_panel_2.insets = new Insets(0, 0, 5, 0);
		gbc_panel_2.fill = GridBagConstraints.VERTICAL;
		gbc_panel_2.gridx = 2;
		gbc_panel_2.gridy = 0;
		this.add(panel_2, gbc_panel_2);
		//this.getContentPane().add(panel_2, gbc_panel_2);
		GridBagLayout gbl_panel_2 = new GridBagLayout();
		gbl_panel_2.columnWidths = new int[]{188, 0};
		gbl_panel_2.rowHeights = new int[]{0, 0, 0, 0};
		gbl_panel_2.columnWeights = new double[]{0.0, Double.MIN_VALUE};
		gbl_panel_2.rowWeights = new double[]{0.0, 0.0, 0.0, Double.MIN_VALUE};
		panel_2.setLayout(gbl_panel_2);
		
		JLabel lblIcdAnalysis = new JLabel("ICD Analysis");
		GridBagConstraints gbc_lblIcdAnalysis = new GridBagConstraints();
		gbc_lblIcdAnalysis.insets = new Insets(0, 0, 5, 0);
		gbc_lblIcdAnalysis.anchor = GridBagConstraints.WEST;
		gbc_lblIcdAnalysis.gridx = 0;
		gbc_lblIcdAnalysis.gridy = 0;
		panel_2.add(lblIcdAnalysis, gbc_lblIcdAnalysis);
		
		icdNoLabel = new JLabel("Number of ICD Affected:");
		GridBagConstraints gbc_icdNoLabel = new GridBagConstraints();
		gbc_icdNoLabel.anchor = GridBagConstraints.WEST;
		gbc_icdNoLabel.insets = new Insets(0, 0, 5, 0);
		gbc_icdNoLabel.gridx = 0;
		gbc_icdNoLabel.gridy = 1;
		panel_2.add(icdNoLabel, gbc_icdNoLabel);
		
		transAllICDArea = new JTextArea();
		transAllICDArea.setEditable(false);
		JScrollPane transAllICDAreaScroll = new JScrollPane (transAllICDArea);
		transAllICDAreaScroll.getVerticalScrollBar().setUI(new NewScrollBarUI());
		transAllICDAreaScroll.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		transAllICDAreaScroll.setMinimumSize(new Dimension(250,250));
		transAllICDAreaScroll.setPreferredSize(new Dimension(250,250));
		GridBagConstraints gbc_transAllICDAreaScroll = new GridBagConstraints();
		gbc_transAllICDAreaScroll.fill = GridBagConstraints.BOTH;
		gbc_transAllICDAreaScroll.gridx = 0;
		gbc_transAllICDAreaScroll.gridy = 2;
		panel_2.add(transAllICDAreaScroll, gbc_transAllICDAreaScroll);
		
		JSeparator separator = new JSeparator();
		GridBagConstraints gbc_separator = new GridBagConstraints();
		gbc_separator.fill = GridBagConstraints.HORIZONTAL;
		gbc_separator.gridwidth = 3;
		gbc_separator.insets = new Insets(0, 0, 5, 5);
		gbc_separator.gridx = 0;
		gbc_separator.gridy = 1;
		add(separator, gbc_separator);
		
		JLabel label_2 = new JLabel("Web Service Provider Analysis");
		GridBagConstraints gbc_label_2 = new GridBagConstraints();
		gbc_label_2.anchor = GridBagConstraints.WEST;
		gbc_label_2.insets = new Insets(0, 0, 5, 5);
		gbc_label_2.gridx = 0;
		gbc_label_2.gridy = 2;
		add(label_2, gbc_label_2);
		
		JPanel panel_3 = new JPanel();
		GridBagConstraints gbc_panel_3 = new GridBagConstraints();
		gbc_panel_3.anchor = GridBagConstraints.NORTHWEST;
		gbc_panel_3.insets = new Insets(0, 0, 5, 5);
		gbc_panel_3.gridx = 0;
		gbc_panel_3.gridy = 3;
		//this.getContentPane().add(panel_3, gbc_panel_3);
		this.add(panel_3, gbc_panel_3);
		GridBagLayout gbl_panel_3 = new GridBagLayout();
		gbl_panel_3.columnWidths = new int[]{188, 0};
		gbl_panel_3.rowHeights = new int[]{0, 0, 0, 0, 0};
		gbl_panel_3.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel_3.rowWeights = new double[]{0.0, 0.0, 0.0, 1.0, Double.MIN_VALUE};
		panel_3.setLayout(gbl_panel_3);
		
		JLabel lblWebServiceProvider = new JLabel("Data Web Service ESB");
		GridBagConstraints gbc_lblWebServiceProvider = new GridBagConstraints();
		gbc_lblWebServiceProvider.insets = new Insets(0, 0, 5, 0);
		gbc_lblWebServiceProvider.anchor = GridBagConstraints.WEST;
		gbc_lblWebServiceProvider.gridx = 0;
		gbc_lblWebServiceProvider.gridy = 0;
		panel_3.add(lblWebServiceProvider, gbc_lblWebServiceProvider);
		
		genDataLbl = new JLabel("Number of systems affected: 0");
		GridBagConstraints gbc_label = new GridBagConstraints();
		gbc_label.anchor = GridBagConstraints.WEST;
		gbc_label.insets = new Insets(0, 0, 5, 0);
		gbc_label.gridx = 0;
		gbc_label.gridy = 1;
		panel_3.add(genDataLbl, gbc_label);
		
		genDataCostLbl = new JLabel("Number of Service-Data ESB Items: 0");
		GridBagConstraints gbc_genDataCostLbl = new GridBagConstraints();
		gbc_genDataCostLbl.anchor = GridBagConstraints.NORTHWEST;
		gbc_genDataCostLbl.insets = new Insets(0, 0, 5, 0);
		gbc_genDataCostLbl.gridx = 0;
		gbc_genDataCostLbl.gridy = 2;
		panel_3.add(genDataCostLbl, gbc_genDataCostLbl);
		
		transAllGenDataArea = new JTextArea();
		transAllGenDataArea.setEditable(false);
		JScrollPane transAllWSPAreaScroll = new JScrollPane (transAllGenDataArea);
		transAllWSPAreaScroll.getVerticalScrollBar().setUI(new NewScrollBarUI());
		transAllWSPAreaScroll.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		transAllWSPAreaScroll.setMinimumSize(new Dimension(250,250));
		transAllWSPAreaScroll.setPreferredSize(new Dimension(250,250));
		GridBagConstraints gbc_transAllWSPAreaScroll = new GridBagConstraints();
		gbc_transAllWSPAreaScroll.anchor = GridBagConstraints.NORTHWEST;
		gbc_transAllWSPAreaScroll.gridx = 0;
		gbc_transAllWSPAreaScroll.gridy = 3;
		panel_3.add(transAllWSPAreaScroll, gbc_transAllWSPAreaScroll);
		
		JPanel panel_4 = new JPanel();
		GridBagConstraints gbc_panel_4 = new GridBagConstraints();
		gbc_panel_4.anchor = GridBagConstraints.NORTHWEST;
		gbc_panel_4.insets = new Insets(0, 0, 5, 5);
		gbc_panel_4.gridx = 1;
		gbc_panel_4.gridy = 3;
		//this.getContentPane().add(panel_4, gbc_panel_4);
		this.add(panel_4, gbc_panel_4);

		GridBagLayout gbl_panel_4 = new GridBagLayout();
		gbl_panel_4.columnWidths = new int[]{0, 0};
		gbl_panel_4.rowHeights = new int[]{0, 0, 0, 0, 0};
		gbl_panel_4.columnWeights = new double[]{0.0, Double.MIN_VALUE};
		gbl_panel_4.rowWeights = new double[]{0.0, 0.0, 0.0, 1.0, Double.MIN_VALUE};
		panel_4.setLayout(gbl_panel_4);
		
		JLabel lblWebServiceConsumer = new JLabel("Business Logic Web Service ESB\r\n");
		GridBagConstraints gbc_lblWebServiceConsumer = new GridBagConstraints();
		gbc_lblWebServiceConsumer.insets = new Insets(0, 0, 5, 0);
		gbc_lblWebServiceConsumer.anchor = GridBagConstraints.WEST;
		gbc_lblWebServiceConsumer.gridx = 0;
		gbc_lblWebServiceConsumer.gridy = 0;
		panel_4.add(lblWebServiceConsumer, gbc_lblWebServiceConsumer);
		
		genBLULbl = new JLabel("Number of systems affected: 0");
		GridBagConstraints gbc_label_1 = new GridBagConstraints();
		gbc_label_1.anchor = GridBagConstraints.WEST;
		gbc_label_1.insets = new Insets(0, 0, 5, 0);
		gbc_label_1.gridx = 0;
		gbc_label_1.gridy = 1;
		panel_4.add(genBLULbl, gbc_label_1);
		
		genBLUCostLbl = new JLabel("Number of Service-BLU ESB Items: 0");
		GridBagConstraints gbc_genBLUCostLbl = new GridBagConstraints();
		gbc_genBLUCostLbl.anchor = GridBagConstraints.NORTHWEST;
		gbc_genBLUCostLbl.insets = new Insets(0, 0, 5, 0);
		gbc_genBLUCostLbl.gridx = 0;
		gbc_genBLUCostLbl.gridy = 2;
		panel_4.add(genBLUCostLbl, gbc_genBLUCostLbl);
		
		transAllGenBLUArea = new JTextArea();
		transAllGenBLUArea.setEditable(false);
		JScrollPane transAllWSCAreaScroll = new JScrollPane (transAllGenBLUArea);
		transAllWSCAreaScroll.getVerticalScrollBar().setUI(new NewScrollBarUI());
		transAllWSCAreaScroll.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		transAllWSCAreaScroll.setMinimumSize(new Dimension(250,250));
		transAllWSCAreaScroll.setPreferredSize(new Dimension(250,250));
		GridBagConstraints gbc_transAllWSCAreaScroll = new GridBagConstraints();
		gbc_transAllWSCAreaScroll.anchor = GridBagConstraints.NORTHWEST;
		gbc_transAllWSCAreaScroll.gridx = 0;
		gbc_transAllWSCAreaScroll.gridy = 3;
		panel_4.add(transAllWSCAreaScroll, gbc_transAllWSCAreaScroll);
		
		JPanel panel_5 = new JPanel();
		GridBagConstraints gbc_panel_5 = new GridBagConstraints();
		gbc_panel_5.anchor = GridBagConstraints.NORTHWEST;
		gbc_panel_5.insets = new Insets(0, 0, 5, 0);
		gbc_panel_5.gridx = 2;
		gbc_panel_5.gridy = 3;
		add(panel_5, gbc_panel_5);
		GridBagLayout gbl_panel_5 = new GridBagLayout();
		gbl_panel_5.columnWidths = new int[]{0, 0};
		gbl_panel_5.rowHeights = new int[]{0, 0, 0, 0, 0};
		gbl_panel_5.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel_5.rowWeights = new double[]{0.0, 0.0, 0.0, 1.0, Double.MIN_VALUE};
		panel_5.setLayout(gbl_panel_5);
		
		JLabel lblDataFederation = new JLabel("Data Federation");
		GridBagConstraints gbc_lblDataFederation = new GridBagConstraints();
		gbc_lblDataFederation.anchor = GridBagConstraints.WEST;
		gbc_lblDataFederation.insets = new Insets(0, 0, 5, 0);
		gbc_lblDataFederation.gridx = 0;
		gbc_lblDataFederation.gridy = 0;
		panel_5.add(lblDataFederation, gbc_lblDataFederation);
		
		dataFedLbl = new JLabel("Number of systems affected: 0");
		GridBagConstraints gbc_label_4 = new GridBagConstraints();
		gbc_label_4.anchor = GridBagConstraints.WEST;
		gbc_label_4.insets = new Insets(0, 0, 5, 0);
		gbc_label_4.gridx = 0;
		gbc_label_4.gridy = 1;
		panel_5.add(dataFedLbl , gbc_label_4);
		
		dataFedCostLbl = new JLabel("Number of systems affected: 0");
		GridBagConstraints gbc_dataFedCostLbl = new GridBagConstraints();
		gbc_dataFedCostLbl.anchor = GridBagConstraints.NORTHWEST;
		gbc_dataFedCostLbl.insets = new Insets(0, 0, 5, 0);
		gbc_dataFedCostLbl.gridx = 0;
		gbc_dataFedCostLbl.gridy = 2;
		panel_5.add(dataFedCostLbl, gbc_dataFedCostLbl);
		
		transAllDataFedArea = new JTextArea();
		JScrollPane scrollPane = new JScrollPane(transAllDataFedArea);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		scrollPane.setMinimumSize(new Dimension(250,250));
		scrollPane.setPreferredSize(new Dimension(250,250));
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.anchor = GridBagConstraints.NORTHWEST;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 3;
		panel_5.add(scrollPane, gbc_scrollPane);
		
		JPanel panel_6 = new JPanel();
		GridBagConstraints gbc_panel_6 = new GridBagConstraints();
		gbc_panel_6.anchor = GridBagConstraints.NORTHWEST;
		gbc_panel_6.insets = new Insets(0, 0, 0, 5);
		gbc_panel_6.gridx = 0;
		gbc_panel_6.gridy = 4;
		add(panel_6, gbc_panel_6);
		GridBagLayout gbl_panel_6 = new GridBagLayout();
		gbl_panel_6.columnWidths = new int[]{0, 0};
		gbl_panel_6.rowHeights = new int[]{0, 0, 0, 0, 0};
		gbl_panel_6.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel_6.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		panel_6.setLayout(gbl_panel_6);
		
		JLabel lblDataConsumers = new JLabel("Data Consumer");
		GridBagConstraints gbc_lblDataConsumers = new GridBagConstraints();
		gbc_lblDataConsumers.anchor = GridBagConstraints.WEST;
		gbc_lblDataConsumers.insets = new Insets(0, 0, 5, 0);
		gbc_lblDataConsumers.gridx = 0;
		gbc_lblDataConsumers.gridy = 0;
		panel_6.add(lblDataConsumers, gbc_lblDataConsumers);
		
		dataConLbl  = new JLabel("Number of systems affected: 0");
		GridBagConstraints gbc_label_6 = new GridBagConstraints();
		gbc_label_6.anchor = GridBagConstraints.WEST;
		gbc_label_6.insets = new Insets(0, 0, 5, 0);
		gbc_label_6.gridx = 0;
		gbc_label_6.gridy = 1;
		panel_6.add(dataConLbl , gbc_label_6);
		
		dataConCostLbl = new JLabel("Number of systems affected: 0");
		GridBagConstraints gbc_dataConCostLbl = new GridBagConstraints();
		gbc_dataConCostLbl.anchor = GridBagConstraints.NORTHWEST;
		gbc_dataConCostLbl.insets = new Insets(0, 0, 5, 0);
		gbc_dataConCostLbl.gridx = 0;
		gbc_dataConCostLbl.gridy = 2;
		panel_6.add(dataConCostLbl, gbc_dataConCostLbl);
		
		transAllDataConArea = new JTextArea();
		JScrollPane scrollPane_1 = new JScrollPane(transAllDataConArea);
		scrollPane_1.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_1.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		scrollPane_1.setMinimumSize(new Dimension(250,250));
		scrollPane_1.setPreferredSize(new Dimension(250,250));
		GridBagConstraints gbc_scrollPane_1 = new GridBagConstraints();
		gbc_scrollPane_1.anchor = GridBagConstraints.NORTHWEST;
		gbc_scrollPane_1.gridx = 0;
		gbc_scrollPane_1.gridy = 3;
		panel_6.add(scrollPane_1, gbc_scrollPane_1);
		
		JPanel panel_7 = new JPanel();
		GridBagConstraints gbc_panel_7 = new GridBagConstraints();
		gbc_panel_7.anchor = GridBagConstraints.NORTHWEST;
		gbc_panel_7.insets = new Insets(0, 0, 0, 5);
		gbc_panel_7.gridx = 1;
		gbc_panel_7.gridy = 4;
		add(panel_7, gbc_panel_7);
		GridBagLayout gbl_panel_7 = new GridBagLayout();
		gbl_panel_7.columnWidths = new int[]{0, 0};
		gbl_panel_7.rowHeights = new int[]{0, 0, 0, 0, 0};
		gbl_panel_7.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel_7.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		panel_7.setLayout(gbl_panel_7);
		
		JLabel lblBluProvider = new JLabel("Business Logic Provider");
		GridBagConstraints gbc_lblBluProvider = new GridBagConstraints();
		gbc_lblBluProvider.anchor = GridBagConstraints.WEST;
		gbc_lblBluProvider.insets = new Insets(0, 0, 5, 0);
		gbc_lblBluProvider.gridx = 0;
		gbc_lblBluProvider.gridy = 0;
		panel_7.add(lblBluProvider, gbc_lblBluProvider);
		
		bluProLbl  = new JLabel("Number of systems affected: 0");
		GridBagConstraints gbc_label_8 = new GridBagConstraints();
		gbc_label_8.anchor = GridBagConstraints.WEST;
		gbc_label_8.insets = new Insets(0, 0, 5, 0);
		gbc_label_8.gridx = 0;
		gbc_label_8.gridy = 1;
		panel_7.add(bluProLbl, gbc_label_8);
		
		bluProCostLbl = new JLabel("Number of System-BLU Provider Items: 0");
		GridBagConstraints gbc_bluProCostLbl = new GridBagConstraints();
		gbc_bluProCostLbl.anchor = GridBagConstraints.NORTHWEST;
		gbc_bluProCostLbl.insets = new Insets(0, 0, 5, 0);
		gbc_bluProCostLbl.gridx = 0;
		gbc_bluProCostLbl.gridy = 2;
		panel_7.add(bluProCostLbl, gbc_bluProCostLbl);
		
		transAllBLUProArea = new JTextArea();
		JScrollPane scrollPane_2 = new JScrollPane(transAllBLUProArea);
		scrollPane_2.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane_2.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		scrollPane_2.setPreferredSize(new Dimension(250, 250));
		scrollPane_2.setMinimumSize(new Dimension(250,250));
		GridBagConstraints gbc_scrollPane_2 = new GridBagConstraints();
		gbc_scrollPane_2.anchor = GridBagConstraints.NORTHWEST;
		gbc_scrollPane_2.gridx = 0;
		gbc_scrollPane_2.gridy = 3;
		panel_7.add(scrollPane_2, gbc_scrollPane_2);
		
		JPanel panel_8 = new JPanel();
		GridBagConstraints gbc_panel_8 = new GridBagConstraints();
		gbc_panel_8.fill = GridBagConstraints.BOTH;
		gbc_panel_8.gridx = 2;
		gbc_panel_8.gridy = 4;
		add(panel_8, gbc_panel_8);
		GridBagLayout gbl_panel_8 = new GridBagLayout();
		gbl_panel_8.columnWidths = new int[]{0, 0};
		gbl_panel_8.rowHeights = new int[]{0, 0, 0, 0};
		gbl_panel_8.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_panel_8.rowWeights = new double[]{0.0, 0.0, 0.0, Double.MIN_VALUE};
		panel_8.setLayout(gbl_panel_8);
		
		JLabel lblTotalServiceAnalysis = new JLabel("Total Service Analysis");
		GridBagConstraints gbc_lblTotalServiceAnalysis = new GridBagConstraints();
		gbc_lblTotalServiceAnalysis.anchor = GridBagConstraints.WEST;
		gbc_lblTotalServiceAnalysis.insets = new Insets(0, 0, 5, 0);
		gbc_lblTotalServiceAnalysis.gridx = 0;
		gbc_lblTotalServiceAnalysis.gridy = 0;
		panel_8.add(lblTotalServiceAnalysis, gbc_lblTotalServiceAnalysis);
	}
	
	/**
	 * Sets all Swing component values as obtained from inputValues/retValues.
	 */
	public void setValues()
	{
		Vector sysV = (Vector) inputValues.get("System");
		Vector icdV = (Vector) inputValues.get("InterfaceControlDocument");
		Vector dataV = (Vector) inputValues.get("DataObject");
		Vector dataGenV = (Vector) inputValues.get("dataGeneric");
		Vector bluGenV = (Vector) inputValues.get("bluGeneric");
		Vector bluProV = (Vector) inputValues.get("bluProvider");
		Vector dataFedV = (Vector) inputValues.get("dataFed");
		Vector dataConV = (Vector) inputValues.get("dataCon");
		double dataGenCost = ((Double)(inputValues.get("dataGenericCost"))).doubleValue();
		double bluGenCost = ((Double)(inputValues.get("bluGenericCost"))).doubleValue();
		double bluProCost = ((Double)( inputValues.get("bluProviderCost"))).doubleValue();
		double dataFedCost = ((Double)(inputValues.get("dataFedCost"))).doubleValue();
		double dataConCost = ((Double) (inputValues.get("dataConCost"))).doubleValue();
		
		sysNoLabel.setText("Number of systems affected: " + sysV.size());
		dataNoLabel.setText("Number of data objects affected: " + dataV.size());
		icdNoLabel.setText("Number of interfaces affected: " + icdV.size());
		genDataLbl.setText("Number of Service-Data ESB Items: " + dataGenV.size());
		genBLULbl.setText("Number of Service-BLU ESB Items: " + bluGenV.size());
		bluProLbl.setText("Number of System-BLU Provider Items: " + bluProV.size());
		dataFedLbl.setText("Number of System-Data Federation Items: " + dataFedV.size());
		dataConLbl.setText("Number of System-Data Consumer Items: " + dataConV.size());
		genDataCostLbl.setText("Cost of all items: $" + dataGenCost);
		genBLUCostLbl.setText("Cost of all items: $" + bluGenCost);
		bluProCostLbl.setText("Cost of all items: $" + bluProCost);
		dataFedCostLbl.setText("Cost of all items: $" + dataFedCost);
		dataConCostLbl.setText("Cost of all items: $" + dataConCost);
		
		String sysString = processVectorForTextArea(sysV);
		String dataString = processVectorForTextArea(dataV);
		String icdString = processVectorForTextArea(icdV);
		String dataGenString = processVectorForTextArea(dataGenV);
		String bluGenString = processVectorForTextArea(bluGenV);
		String bluProString = processVectorForTextArea(bluProV);
		String dataFedString = processVectorForTextArea(dataFedV);
		String dataConString = processVectorForTextArea(dataConV);
		
		transAllSysArea.setText(sysString);
		transAllDataArea.setText(dataString);
		transAllICDArea.setText(icdString);
		transAllGenDataArea.setText(dataGenString);
		transAllGenBLUArea.setText(bluGenString);
		transAllBLUProArea.setText(bluProString);
		transAllDataFedArea.setText(dataFedString);
		transAllDataConArea.setText(dataConString);
	}
	
	/**
	 * Formats list of Strings into consolidated newline delimited String.
	 * 
	 * @param v Vector	list of Strings to be appended
	 * @return String	list of Strings appended, delimited with newlines
	 * */
	public String processVectorForTextArea (Vector v)
	{
		String str = "";
		for (int i=0;i<v.size();i++)
		{
			str = str + (String) v.get(i) + "\n";
		}
		return str;
	}
}
