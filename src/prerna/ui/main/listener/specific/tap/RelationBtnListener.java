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

import javax.swing.JComponent;
import javax.swing.JTextArea;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.specific.tap.RelationFunction;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.RelationPlaySheet;
import prerna.ui.helpers.AlgorithmRunner;

/**
 * Activates when "Generate Relations" button is pushed
 */
public class RelationBtnListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(RelationBtnListener.class.getName());

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
		optimizer.setDataList(playSheet.dataSelectPanel.getSelectedData());
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