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
package prerna.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.swing.DefaultComboBoxModel;
import javax.swing.DefaultListModel;
import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JList;

/**
 * This class opens a thread and watches a specific SMSS file.
 */
public class SMSSFileWatcher extends AbstractFileWatcher {

	/**
	 * Processes SMSS files.
	 * @param	Name of the file.
	 */
	@Override
	public void process(String fileName) {
		loadNewDB(fileName);
	}
	
	/**
	 * Returns an array of strings naming the files in the directory.
	 * Goes through list and loads an existing database.
	 */
	public void loadExistingDB()
	{
		File dir = new File(folderToWatch);
		String [] fileNames = dir.list(this);
		for(int fileIdx = 0;fileIdx < fileNames.length;fileIdx++)
		{
			try{
				String fileName = folderToWatch + "/" + fileNames[fileIdx];
				loadNewDB(fileNames[fileIdx]);
				//Utility.loadEngine(fileName, prop);				
			}catch(RuntimeException ex)
			{
				ex.printStackTrace();
				logger.fatal("Engine Failed " + "./db/" + fileNames[fileIdx]);
			}
		}	

	}
	
	/**
	 * Loads a new database by setting a specific engine with associated properties.
	 * @param 	Specifies properties to load 
	 */
	public void loadNewDB(String newFile)
	{
		FileInputStream fileIn = null;
		try {
			Properties prop = new Properties();
			fileIn = new FileInputStream(folderToWatch + "/"  +  newFile);
			prop.load(fileIn);
	
			Utility.loadEngine(folderToWatch+ "/" +  newFile, prop);
			String engineName = prop.getProperty(Constants.ENGINE);
			//check if hidden database
			boolean hidden = (prop.getProperty(Constants.HIDDEN_DATABASE) != null && Boolean.parseBoolean(prop.getProperty(Constants.HIDDEN_DATABASE)));
			if(!hidden){
				JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
				DefaultListModel listModel = (DefaultListModel) list.getModel();
				listModel.addElement(engineName);
				//list.setModel(listModel);
				list.setSelectedIndex(0);
				list.repaint();
			}
			
			// initialize combo box for cost db update
			JComboBox changedDBComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.CHANGED_DB_COMBOBOX);
			DefaultComboBoxModel changedDBComboBoxModel = (DefaultComboBoxModel) changedDBComboBox.getModel();
			changedDBComboBoxModel.addElement(engineName);
			changedDBComboBox.repaint();
			
			JComboBox costDBComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.COST_DB_COMBOBOX);
			DefaultComboBoxModel costDBComboBoxModel = (DefaultComboBoxModel) costDBComboBox.getModel();
			costDBComboBoxModel.addElement(engineName);
			costDBComboBox.repaint();
			
			// initialize combo box for aggregating tap services into tap cost
			JComboBox selectTapCoreForAggregationComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_SERVICES_AGGREGATION_CORE_COMBO_BOX);
			DefaultComboBoxModel selectTapCoreForAggregationComboBoxModel = (DefaultComboBoxModel) selectTapCoreForAggregationComboBox.getModel();
			selectTapCoreForAggregationComboBoxModel.addElement(engineName);
			selectTapCoreForAggregationComboBox.repaint();
			
			JComboBox selectTapServicesComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_SERVICES_AGGREGATION_SERVICE_COMBO_BOX);
			DefaultComboBoxModel selectTapServicesComboBoxModel = (DefaultComboBoxModel) selectTapServicesComboBox.getModel();
			selectTapServicesComboBoxModel.addElement(engineName);
			selectTapServicesComboBox.repaint();
			
			// initialize combo box for creating future interface db
			JComboBox selectHRCoreForFutureInterfaceComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.HR_CORE_FUTURE_INTERFACE_DATABASE_CORE_COMBO_BOX);
			DefaultComboBoxModel selectHRCoreForFutureInterfaceComboBoxModel = (DefaultComboBoxModel) selectHRCoreForFutureInterfaceComboBox.getModel();
			selectHRCoreForFutureInterfaceComboBoxModel.addElement(engineName);
			selectHRCoreForFutureInterfaceComboBox.repaint();
			
			JComboBox selectTapFutureInterfaceComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_FUTURE_INTERFACE_DATABASE_COMBO_BOX);
			DefaultComboBoxModel selectTapFutureInterfaceComboBoxModel = (DefaultComboBoxModel) selectTapFutureInterfaceComboBox.getModel();
			selectTapFutureInterfaceComboBoxModel.addElement(engineName);
			selectTapServicesComboBox.repaint();
			
			JComboBox selectTapFutureCostInterfaceComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_FUTURE_COST_INTERFACE_DATABASE_COMBO_BOX);
			DefaultComboBoxModel selectTapFutureCostInterfaceComboBoxModel = (DefaultComboBoxModel) selectTapFutureCostInterfaceComboBox.getModel();
			selectTapFutureCostInterfaceComboBoxModel.addElement(engineName);
			selectTapFutureCostInterfaceComboBox.repaint();
			
			
			// initialize combo box for export db information
			JComboBox exportDataDBComboBox = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.EXPORT_LOAD_SHEET_SOURCE_COMBOBOX);
			DefaultComboBoxModel exportDataDBComboBoxModel = (DefaultComboBoxModel) exportDataDBComboBox.getModel();
			exportDataDBComboBoxModel.addElement(engineName);
			exportDataDBComboBox.repaint();
			
			//initialize combo box for common subgraph
			JComboBox commonSubgraphComboBox0 = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.COMMON_SUBGRAPH_COMBO_BOX_0);
			DefaultComboBoxModel commonSubgraphComboBox0Model = (DefaultComboBoxModel) commonSubgraphComboBox0.getModel();
			commonSubgraphComboBox0Model.addElement(engineName);
			commonSubgraphComboBox0.repaint();
			
			JComboBox commonSubgraphComboBox1 = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.COMMON_SUBGRAPH_COMBO_BOX_1);
			DefaultComboBoxModel commonSubgraphComboBox1Model = (DefaultComboBoxModel) commonSubgraphComboBox1.getModel();
			commonSubgraphComboBox1Model.addElement(engineName);
			commonSubgraphComboBox1.repaint();
			
			//initialize combo box for question modification
			JComboBox questionDatabaseSelector = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.QUESTION_DB_SELECTOR);
			DefaultComboBoxModel questionDatabaseSelectorModel = (DefaultComboBoxModel) questionDatabaseSelector.getModel();
			questionDatabaseSelectorModel.addElement(engineName);
			questionDatabaseSelector.repaint();
			
			
			JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
					Constants.MAIN_FRAME);
			frame2.repaint();
			
		} catch(IOException ex) {
			// TODO: Specify exception
			ex.printStackTrace();
		}finally{
			try{
				if(fileIn!=null)
					fileIn.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * Used in the starter class for processing SMSS files.
	 */
	@Override
	public void loadFirst()
	{
		File dir = new File(folderToWatch);
		String [] fileNames = dir.list(this);
		for(int fileIdx = 0;fileIdx < fileNames.length;fileIdx++)
		{
			try{
				String fileName = folderToWatch + fileNames[fileIdx];
				Properties prop = new Properties();
				process(fileNames[fileIdx]);
			}catch(RuntimeException ex)
			{
				logger.fatal("Engine Failed " + folderToWatch + "/" + fileNames[fileIdx]);
			}
		}
	}

	
	/**
	 * Processes new SMSS files.
	 */
	@Override
	public void run()
	{
		logger.info("Starting thread");
		synchronized(monitor)
		{
			super.run();
		}
	}

}
