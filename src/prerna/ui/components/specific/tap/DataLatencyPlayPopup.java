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
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.beans.PropertyVetoException;

import javax.swing.JButton;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JLabel;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.DataLatencyPerformer;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.main.listener.specific.tap.DataLatencyPausePlayButtonListener;
import prerna.ui.main.listener.specific.tap.DataLatencyPlayInternalFrameListener;
import prerna.ui.main.listener.specific.tap.DataLatencyResetButtonListener;
import prerna.ui.main.listener.specific.tap.DataLatencyStopButtonListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Creates an internal frame within SEMOSS that displays information about data latency.
 */
public class DataLatencyPlayPopup extends JInternalFrame implements Runnable {

	public double hoursValue;
	public GraphPlaySheet ps = null;
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(DataLatencyPlayPopup.class.getName());
	DataLatencyPerformer latePerf;
	private JTextField dataLatencyHoursTextField;
	private JTextField dataLatencyWeeksTextField;
	private JTextField dataLatencyDaysTextField;
	double maxValue = 1176;
	public Thread thread;
	JDesktopPane desktopPane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
	
	/**
	 * Constructor for DataLatencyPlayPopup.
	 * @param hours Double
	 * @param p GraphPlaySheet
	 * @param picked DBCMVertex[]
	 */
	public DataLatencyPlayPopup(Double hours, GraphPlaySheet p, SEMOSSVertex[] picked){
		hoursValue = hours;
		ps = p;
		pickedVertex = picked;
		latePerf = new DataLatencyPerformer(ps, pickedVertex);
		latePerf.fillHashesWithValuesUpTo(maxValue);
	}
	
	/**
	 * Sets the thread.
	 * @param t Thread.
	 */
	public void setThread(Thread t){
		thread = t;
	}
	
	/**
	 * Runs data latency.
	 */
	public void run(){
		start();
	}
	
	/**
	 * While the hours value is less than the max value, run the latency calculation.
	 */
	public void start(){
		while (hoursValue <= maxValue){
			runLatency(hoursValue);
			hoursValue=hoursValue + 5;
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Run the latency calculations.
	 * @param value 	Data latency value.
	 */
	private void runLatency(double value){

		latePerf.setValue(value);
		
		latePerf.executeFromHash();
		
		setTextValues(value);
	}
	
	/**
	 * Sets the value for hours and runs the calculation based on that amount.
	 * @param hours 	Number of hours, in double format.
	 */
	public void setHoursValue(double hours){
		hoursValue = hours;
		runLatency(hours);
	}

	/**
	 * Sets the slider values for weeks, days, and hours.
	 * @param sliderValue 	Double that gives the total hours used for the slider value.
	 */
	private void setTextValues(double sliderValue){
		double totalHours = sliderValue;
		int weeks = (int) Math.floor(totalHours/168);
		int days = (int) Math.floor((totalHours-(weeks*168))/24);
		int hours = (int) (totalHours-(weeks*168)-(days*24));
		dataLatencyWeeksTextField.setText(Integer.toString(weeks));
		dataLatencyDaysTextField.setText(Integer.toString(days));
		dataLatencyHoursTextField.setText(Integer.toString(hours));
	}

	/**
	 * Creates the data latency slider.
	 */
	public void create(){
		this.setTitle("Data Latency Slider");
		this.setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
		this.setResizable(true);
		this.setClosable(true);
		this.setBounds(100, 100, 200, 125);
		this.setLocation(0, desktopPane.getHeight()-this.getHeight());
		GridBagLayout gridBagLayout_2 = new GridBagLayout();
		gridBagLayout_2.columnWidths = new int[]{0, 0, 10, 75, 0, 0};
		gridBagLayout_2.rowHeights = new int[]{0, 0, 0, 0, 0, 0};
		gridBagLayout_2.columnWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		gridBagLayout_2.rowWeights = new double[]{0.0, 0.0, 0.0, 0.0, 0.0, Double.MIN_VALUE};
		this.getContentPane().setLayout(gridBagLayout_2);
		
		JButton btnPlay = new JButton("Play");
		GridBagConstraints gbc_btnPlay = new GridBagConstraints();
		gbc_btnPlay.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnPlay.insets = new Insets(0, 0, 5, 5);
		gbc_btnPlay.gridx = 3;
		gbc_btnPlay.gridy = 1;
		btnPlay.addActionListener(new DataLatencyPausePlayButtonListener(ps, this));
		this.getContentPane().add(btnPlay, gbc_btnPlay);
		
		JLabel lblDays = new JLabel("Days");
		GridBagConstraints gbc_lblDays = new GridBagConstraints();
		gbc_lblDays.anchor = GridBagConstraints.WEST;
		gbc_lblDays.insets = new Insets(0, 0, 5, 5);
		gbc_lblDays.gridx = 0;
		gbc_lblDays.gridy = 2;
		this.getContentPane().add(lblDays, gbc_lblDays);
		
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
		this.getContentPane().add(dataLatencyDaysTextField, gbc_dataLatencyDaysTextField);
		dataLatencyDaysTextField.setColumns(10);
		
		JButton btnReset = new JButton("Reset");
		GridBagConstraints gbc_btnReset = new GridBagConstraints();
		gbc_btnReset.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnReset.insets = new Insets(0, 0, 5, 5);
		gbc_btnReset.gridx = 3;
		gbc_btnReset.gridy = 2;
		btnReset.addActionListener(new DataLatencyResetButtonListener(ps, this, btnPlay));
		this.getContentPane().add(btnReset, gbc_btnReset);
		
		JLabel lblWeeks = new JLabel("Weeks");
		GridBagConstraints gbc_lblWeeks = new GridBagConstraints();
		gbc_lblWeeks.anchor = GridBagConstraints.WEST;
		gbc_lblWeeks.insets = new Insets(0, 0, 5, 5);
		gbc_lblWeeks.gridx = 0;
		gbc_lblWeeks.gridy = 3;
		this.getContentPane().add(lblWeeks, gbc_lblWeeks);
		
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
		this.getContentPane().add(dataLatencyWeeksTextField, gbc_dataLatencyWeeksTextField);
		dataLatencyWeeksTextField.setColumns(10);
		//panel.add(dataLatencyInternalFrame);
		//panel.add(dataLatencyInternalFrame, gbc_dataLatencySlider);

		JLabel lblHours_1 = new JLabel("Hours");
		GridBagConstraints gbc_lblHours_1 = new GridBagConstraints();
		gbc_lblHours_1.anchor = GridBagConstraints.WEST;
		gbc_lblHours_1.insets = new Insets(0, 0, 5, 5);
		gbc_lblHours_1.gridx = 0;
		gbc_lblHours_1.gridy = 1;
		this.getContentPane().add(lblHours_1, gbc_lblHours_1);
		
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
		this.getContentPane().add(dataLatencyHoursTextField, gbc_dataLatencyHoursTextField);
		dataLatencyHoursTextField.setColumns(10);
		
		JButton btnStop = new JButton("Stop");
		GridBagConstraints gbc_btnStop = new GridBagConstraints();
		gbc_btnStop.fill = GridBagConstraints.HORIZONTAL;
		gbc_btnStop.insets = new Insets(0, 0, 5, 5);
		gbc_btnStop.gridx = 3;
		gbc_btnStop.gridy = 3;
		this.getContentPane().add(btnStop, gbc_btnStop);
		btnStop.addActionListener(new DataLatencyStopButtonListener(ps, this));
		this.addInternalFrameListener(new DataLatencyPlayInternalFrameListener());
		
		ps.setDataLatencyPlayPopUp(this);
	}
	
	/**
	 * Displays the results of the data latency calculations in the internal frame.
	 */
	public void display(){
		this.setVisible(true);
		desktopPane.add(this);
		this.toFront();
		try {
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
}
