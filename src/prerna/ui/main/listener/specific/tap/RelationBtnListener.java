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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.ArrayList;

import javax.swing.JComponent;
import javax.swing.JTextArea;

import org.apache.log4j.Logger;

import prerna.algorithm.api.IAlgorithm;
import prerna.algorithm.impl.specific.tap.RelationFunction;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.DHMSMDataBLUSelectPanel;
import prerna.ui.components.specific.tap.DHMSMSystemSelectPanel;
import prerna.ui.components.specific.tap.RelationPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;
import prerna.util.Utility;

/**
 */
public class RelationBtnListener implements IChakraListener {
	
	Logger logger = Logger.getLogger(getClass());

	RelationPlaySheet playSheet;
	JTextArea consoleArea;
	
	RelationFunction optimizer;
	
	/**
	 * Method actionPerformed.
	 * @param arg0 ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		logger.info("Generate Relations Button Pushed");
		
		this.optimizer = new RelationFunction();
		optimizer.setPlaySheet(playSheet);
		optimizer.setSysList(playSheet.systemSelectPanel.getSelectedSystems());
		optimizer.setDataList(playSheet.dataBLUSelectPanel.getSelectedData());
		optimizer.setRDFEngine(playSheet.engine);
		AlgorithmRunner runner = new AlgorithmRunner(optimizer);
		Thread playThread = new Thread(runner);
		playThread.start();
		}
		
	/**
	 * Method setOptPlaySheet.
	 * @param sheet RelationPlaySheet
	 */
	public void setPlaySheet(RelationPlaySheet sheet)
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