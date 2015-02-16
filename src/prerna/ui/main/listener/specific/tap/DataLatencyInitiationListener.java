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
package prerna.ui.main.listener.specific.tap;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyVetoException;
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JSlider;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Opens the Data Latency Analysis user interface when JMenuItem "Data Latency Analysis" is selected by the user
 */
public class DataLatencyInitiationListener implements ActionListener{

	GraphPlaySheet ps = null;
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(DataLatencyInitiationListener.class.getName());
	private JTextField dataLatencyHoursTextField;
	private JTextField dataLatencyWeeksTextField;
	private JTextField dataLatencyDaysTextField;
	
	/**
	 * Constructor for DataLatencyInitiationListener.
	 * @param p 		GraphPlaySheet
	 * @param pickedV	DBCMVertex[]
	 */
	public DataLatencyInitiationListener(GraphPlaySheet p, SEMOSSVertex[] pickedV){
		ps = p;
		pickedVertex = pickedV;
	}

	/**
	 * Opens the Data Latency Analysis user interface when JMenuItem "Data Latency Analysis" is selected by the user
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		logger.info("Beginning Data Latency Analysis");
		//add the data latency slider popup to the desktop pane
		
		if(ps.dataLatencyPopUp == null || !ps.dataLatencyPopUp.isVisible()){
			addInternalFrame();
		}
		else{
			JSlider slider = (JSlider) ps.dataLatencyPopUp.getContentPane().getComponent(0);
			DataLatencySliderListener listener = (DataLatencySliderListener) slider.getChangeListeners()[0];
			listener.setPickedVertex(pickedVertex);
			ps.dataLatencyPopUp.toFront();
			try {
				ps.dataLatencyPopUp.setSelected(true);
			} catch (PropertyVetoException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	//TODO: addInternalFame should be a separate class and not in the listener
	
	/**
	 * Configure the visuals of the Data Latency Analysis user interface
	 */
	private void addInternalFrame(){
		JDesktopPane desktopPane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		desktopPane.getHeight();
		JInternalFrame dataLatencyInternalFrame = new JInternalFrame("Data Latency Slider");
		dataLatencyInternalFrame.setResizable(true);
		dataLatencyInternalFrame.setClosable(true);
		dataLatencyInternalFrame.setBounds(100, 100, 800, 125);
		dataLatencyInternalFrame.setLocation(0, desktopPane.getHeight()-dataLatencyInternalFrame.getHeight());
		desktopPane.add(dataLatencyInternalFrame);
		GridBagLayout gridBagLayout_2 = new GridBagLayout();
		gridBagLayout_2.columnWidths = new int[]{0, 0, 0, 0};
		gridBagLayout_2.rowHeights = new int[]{0, 0, 0, 0, 0, 0};
		gridBagLayout_2.columnWeights = new double[]{0.0, 0.0, 1.0, Double.MIN_VALUE};
		gridBagLayout_2.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		dataLatencyInternalFrame.getContentPane().setLayout(gridBagLayout_2);
		
		JLabel lblDays = new JLabel("Days");
		GridBagConstraints gbc_lblDays = new GridBagConstraints();
		gbc_lblDays.anchor = GridBagConstraints.WEST;
		gbc_lblDays.insets = new Insets(0, 0, 5, 5);
		gbc_lblDays.gridx = 0;
		gbc_lblDays.gridy = 2;
		dataLatencyInternalFrame.getContentPane().add(lblDays, gbc_lblDays);
		
		dataLatencyDaysTextField = new JTextField();
		dataLatencyDaysTextField.setFont(new Font("Tahoma", Font.PLAIN, 9));
		dataLatencyDaysTextField.setText("0");
		dataLatencyDaysTextField.setName(Constants.DATA_LATENCY_DAYS_TEXT);
		dataLatencyDaysTextField.setEditable(false);
		dataLatencyDaysTextField.setMinimumSize(new Dimension(30, 20));
		GridBagConstraints gbc_dataLatencyDaysTextField = new GridBagConstraints();
		gbc_dataLatencyDaysTextField.anchor = GridBagConstraints.WEST;
		gbc_dataLatencyDaysTextField.insets = new Insets(0, 0, 5, 5);
		gbc_dataLatencyDaysTextField.gridx = 1;
		gbc_dataLatencyDaysTextField.gridy = 2;
		dataLatencyInternalFrame.getContentPane().add(dataLatencyDaysTextField, gbc_dataLatencyDaysTextField);
		dataLatencyDaysTextField.setColumns(10);
		
		JLabel lblWeeks = new JLabel("Weeks");
		GridBagConstraints gbc_lblWeeks = new GridBagConstraints();
		gbc_lblWeeks.anchor = GridBagConstraints.WEST;
		gbc_lblWeeks.insets = new Insets(0, 0, 5, 5);
		gbc_lblWeeks.gridx = 0;
		gbc_lblWeeks.gridy = 3;
		dataLatencyInternalFrame.getContentPane().add(lblWeeks, gbc_lblWeeks);
		
		dataLatencyWeeksTextField = new JTextField();
		dataLatencyWeeksTextField.setFont(new Font("Tahoma", Font.PLAIN, 9));
		dataLatencyWeeksTextField.setText("0");
		dataLatencyWeeksTextField.setName(Constants.DATA_LATENCY_WEEKS_TEXT);
		dataLatencyWeeksTextField.setEditable(false);
		dataLatencyWeeksTextField.setMinimumSize(new Dimension(30, 20));
		GridBagConstraints gbc_dataLatencyWeeksTextField = new GridBagConstraints();
		gbc_dataLatencyWeeksTextField.anchor = GridBagConstraints.WEST;
		gbc_dataLatencyWeeksTextField.insets = new Insets(0, 0, 5, 5);
		gbc_dataLatencyWeeksTextField.gridx = 1;
		gbc_dataLatencyWeeksTextField.gridy = 3;
		dataLatencyInternalFrame.getContentPane().add(dataLatencyWeeksTextField, gbc_dataLatencyWeeksTextField);
		dataLatencyWeeksTextField.setColumns(10);
		//panel.add(dataLatencyInternalFrame);
		//panel.add(dataLatencyInternalFrame, gbc_dataLatencySlider);

		JSlider dataLatencySlider = new JSlider();
		dataLatencySlider.setPreferredSize(new Dimension(5500, 23));
		int max = 1000;
		int min = 0;
		dataLatencySlider.setPaintTicks(true);
		dataLatencySlider.setMajorTickSpacing(100);
		dataLatencySlider.setMinorTickSpacing(10);
		dataLatencySlider.setSnapToTicks(false);
		dataLatencySlider.setMaximum(max);
		dataLatencySlider.setMinimum(min);
		GridBagConstraints gbc_dataLatencySlider = new GridBagConstraints();
		gbc_dataLatencySlider.gridheight = 2;
		gbc_dataLatencySlider.insets = new Insets(2, 0, 5, 0);
		gbc_dataLatencySlider.fill = GridBagConstraints.HORIZONTAL;
		gbc_dataLatencySlider.gridx = 2;
		gbc_dataLatencySlider.gridy = 1;
		//dataLatencySlider label table
		Hashtable<Integer, JComponent> dataLatencySliderLabelTable = dataLatencySlider.createStandardLabels(50);
		dataLatencySlider.setLabelTable( dataLatencySliderLabelTable );
		
		JLabel lblSetTimeElapsed = new JLabel("Set time elapsed since data creation");
		GridBagConstraints gbc_lblSetTimeElapsed = new GridBagConstraints();
		gbc_lblSetTimeElapsed.gridwidth = 3;
		gbc_lblSetTimeElapsed.anchor = GridBagConstraints.WEST;
		gbc_lblSetTimeElapsed.insets = new Insets(5, 0, 5, 0);
		gbc_lblSetTimeElapsed.gridx = 0;
		gbc_lblSetTimeElapsed.gridy = 0;
		dataLatencyInternalFrame.getContentPane().add(lblSetTimeElapsed, gbc_lblSetTimeElapsed);
		
		JLabel lblHours_1 = new JLabel("Hours");
		GridBagConstraints gbc_lblHours_1 = new GridBagConstraints();
		gbc_lblHours_1.anchor = GridBagConstraints.WEST;
		gbc_lblHours_1.insets = new Insets(0, 0, 5, 5);
		gbc_lblHours_1.gridx = 0;
		gbc_lblHours_1.gridy = 1;
		dataLatencyInternalFrame.getContentPane().add(lblHours_1, gbc_lblHours_1);
		
		JLabel lblHours = new JLabel("Hours");
		GridBagConstraints gbc_lblHours = new GridBagConstraints();
		gbc_lblHours.insets = new Insets(0, 0, 5, 0);
		gbc_lblHours.gridx = 2;
		gbc_lblHours.gridy = 3;
		dataLatencyInternalFrame.getContentPane().add(lblHours, gbc_lblHours);
		
		dataLatencyHoursTextField = new JTextField();
		dataLatencyHoursTextField.setFont(new Font("Tahoma", Font.PLAIN, 9));
		dataLatencyHoursTextField.setText("0");
		dataLatencyHoursTextField.setName(Constants.DATA_LATENCY_HOURS_TEXT);
		dataLatencyHoursTextField.setEditable(false);
		dataLatencyHoursTextField.setMinimumSize(new Dimension(30, 20));
		dataLatencyHoursTextField.setPreferredSize(new Dimension(40, 20));
		GridBagConstraints gbc_dataLatencyHoursTextField = new GridBagConstraints();
		gbc_dataLatencyHoursTextField.anchor = GridBagConstraints.WEST;
		gbc_dataLatencyHoursTextField.insets = new Insets(0, 0, 5, 5);
		gbc_dataLatencyHoursTextField.gridx = 1;
		gbc_dataLatencyHoursTextField.gridy = 1;
		dataLatencyInternalFrame.getContentPane().add(dataLatencyHoursTextField, gbc_dataLatencyHoursTextField);
		dataLatencyHoursTextField.setColumns(10);
		dataLatencySlider.setPaintLabels(true);
		dataLatencyInternalFrame.getContentPane().add(dataLatencySlider, gbc_dataLatencySlider, 0);
		
		dataLatencyInternalFrame.setVisible(true);
		
		ps.setDataLatencyPopUp(dataLatencyInternalFrame);
		DataLatencySliderListener listener = new DataLatencySliderListener(ps, pickedVertex);
		dataLatencySlider.addChangeListener(listener);
		dataLatencySlider.setValue(0);
		
	}

}
