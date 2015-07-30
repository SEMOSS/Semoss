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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;

import javax.swing.JComponent;
import javax.swing.JTextArea;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.specific.tap.SystToBLUHeatmapFunction;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SysToBLUDataGapsPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;

/**
 * Activates when "Update Heat Map" button is pushed
 */
public class SystToBLUHeatmapBtnListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(SystToBLUHeatmapBtnListener.class.getName());

	SysToBLUDataGapsPlaySheet playSheet;
	JTextArea consoleArea;

	SystToBLUHeatmapFunction sysToBluFn;

	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		logger.info("Update Heat Map Button Pushed");

		this.sysToBluFn = new SystToBLUHeatmapFunction();
		sysToBluFn.setPlaySheet(playSheet);
		sysToBluFn.setSysList(playSheet.systemSelectPanel.getSelectedSystems());
		sysToBluFn.setDataList(playSheet.dataSelectPanel.getSelectedData());
		sysToBluFn.setRDFEngine(playSheet.engine);
		AlgorithmRunner runner = new AlgorithmRunner(sysToBluFn);
		Thread playThread = new Thread(runner);
		playThread.start();
	}

	/**
	 * Method setOptPlaySheet.
	 * @param sheet SysToBLUDataGapsPlaySheet
	 */
	public void setPlaySheet(SysToBLUDataGapsPlaySheet sheet)
	{
		this.playSheet = sheet;
	}

	/**
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}

}