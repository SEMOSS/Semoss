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
import java.util.Enumeration;
import java.util.Hashtable;

import javax.swing.JComponent;
import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.poi.specific.DHMSMDataAccessLatencyFileImporter;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.DHMSMSysDecommissionPlaySheet;
import prerna.ui.components.specific.tap.DHMSMSysDecommissionReport;
import prerna.ui.components.specific.tap.SelectRadioButtonPanel;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * Listener for sourceReportGenButton
 */
public class DHMSMDecommissionReportListener implements IChakraListener {

	static final Logger logger = LogManager.getLogger(DHMSMDecommissionReportListener.class.getName());
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		//get data objects that are checked and put them in the data object hash
		Hashtable<String,String> dataAccessTypeHash = new Hashtable<String,String>();
		Hashtable<String,String> dataLatencyTypeHash = new Hashtable<String,String>();

//		JTextField dataAccessLatencyFileField = (JTextField) DIHelper.getInstance().getLocalProp(Constants.SELECT_DATA_ACCESS_FILE_JFIELD);
//		if(dataAccessLatencyFileField.getText()!=null&&dataAccessLatencyFileField.getText().length()>0)
//		{
//			DHMSMDataAccessLatencyFileImporter dataAccessImporter = new DHMSMDataAccessLatencyFileImporter();
//			try {
//				dataAccessImporter.importFile(dataAccessLatencyFileField.getText());
//				dataAccessTypeHash = dataAccessImporter.getDataAccessTypeHash();
//				dataLatencyTypeHash = dataAccessImporter.getDataLatencyTypeHash();
//			} catch (Exception e) {
//				JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
//				JOptionPane.showMessageDialog(playPane, "<html>Error with Selected File.</html>");
//				return;
//			}
//		}
//		else
//		{
			SelectRadioButtonPanel selectRadioPanel = (SelectRadioButtonPanel) DIHelper.getInstance().getLocalProp(Constants.SELECT_RADIO_PANEL);
			Enumeration<String> enumKey = selectRadioPanel.radioIntegratedBoxHash.keys();
			while(enumKey.hasMoreElements()) {
				    String key = enumKey.nextElement();
					JRadioButton radioButton = (JRadioButton) selectRadioPanel.radioIntegratedBoxHash.get(key);
					if (radioButton.isSelected())
					{
						dataAccessTypeHash.put(key, "Integrated");
					}
			}
			Enumeration<String> enumKey2 = selectRadioPanel.radioHybridBoxHash.keys();
			while(enumKey2.hasMoreElements()) {
				    String key = enumKey2.nextElement();
					JRadioButton radioButton = (JRadioButton) selectRadioPanel.radioHybridBoxHash.get(key);
					if (radioButton.isSelected())
					{
						dataAccessTypeHash.put(key, "Hybrid");
					}
			}
			Enumeration<String> enumKey3 = selectRadioPanel.radioManualBoxHash.keys();
			while(enumKey3.hasMoreElements()) {
				    String key = enumKey3.nextElement();
					JRadioButton radioButton = (JRadioButton) selectRadioPanel.radioManualBoxHash.get(key);
					if (radioButton.isSelected())
					{
						dataAccessTypeHash.put(key, "Manual");
					}
			}
			
			Enumeration<String> enumKey4 = selectRadioPanel.radioRealBoxHash.keys();
			while(enumKey4.hasMoreElements()) {
				    String key = enumKey4.nextElement();
					JRadioButton radioButton = (JRadioButton) selectRadioPanel.radioRealBoxHash.get(key);
					if (radioButton.isSelected())
					{
						dataLatencyTypeHash.put(key, "Real");
					}
			}
			Enumeration<String> enumKey5 = selectRadioPanel.radioNearBoxHash.keys();
			while(enumKey5.hasMoreElements()) {
				    String key = enumKey5.nextElement();
					JRadioButton radioButton = (JRadioButton) selectRadioPanel.radioNearBoxHash.get(key);
					if (radioButton.isSelected())
					{
						dataLatencyTypeHash.put(key, "NearReal");
					}
			}
			Enumeration<String> enumKey6 = selectRadioPanel.radioArchiveBoxHash.keys();
			while(enumKey6.hasMoreElements()) {
				    String key = enumKey6.nextElement();
					JRadioButton radioButton = (JRadioButton) selectRadioPanel.radioArchiveBoxHash.get(key);
					if (radioButton.isSelected())
					{
						dataLatencyTypeHash.put(key, "Archive");
					}
			}
			Enumeration<String> enumKey7 = selectRadioPanel.radioIgnoreBoxHash.keys();
			while(enumKey7.hasMoreElements()) {
				    String key = enumKey7.nextElement();
					JRadioButton radioButton = (JRadioButton) selectRadioPanel.radioIgnoreBoxHash.get(key);
					if (radioButton.isSelected())
					{
						dataLatencyTypeHash.put(key, "Ignore");
					}
			}
//		}
		if(dataLatencyTypeHash.isEmpty()&&dataAccessTypeHash.isEmpty())
		{
				JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
				JOptionPane.showMessageDialog(playPane, "<html>Please select at least one data object.</html>");
				return;
		}
	
		logger.info("Processed Data time Hash");
		
		DHMSMSysDecommissionReport sysRep = new DHMSMSysDecommissionReport();
		sysRep.setDataLatencyTypeHash(dataLatencyTypeHash);
		sysRep.setDataAccessTypeHash(dataAccessTypeHash);
		sysRep.runCalculation();
		
		DHMSMSysDecommissionPlaySheet mapVis = new DHMSMSysDecommissionPlaySheet();
		mapVis.setData(sysRep);
		mapVis.createData();
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		mapVis.setJDesktopPane(pane);
		mapVis.createView();
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	}

}
