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
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JList;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.specific.tap.SourceSelectPanel;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class DHMSMFunctionalAreaSelectorListener extends AbstractListener {
	JCheckBox HSDCheckBox;
	JCheckBox HSSCheckBox;
	JCheckBox FHPCheckBox;
	JCheckBox DHMSMCheckBox;
	IEngine engine;

	/**
	 * Determines if the user has selected HSD, HSS, FHP check box's in MHS TAP to include functional areas to include in RFP report
	 * Will populate sourceSelectPanel to show all capabilities for the functional area's selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[])repoList.getSelectedValues();
		engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");

		HSDCheckBox = (JCheckBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.FUNCTIONAL_AREA_CHECKBOX_1+"DHMSM");
		HSSCheckBox = (JCheckBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.FUNCTIONAL_AREA_CHECKBOX_2+"DHMSM");
		FHPCheckBox = (JCheckBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.FUNCTIONAL_AREA_CHECKBOX_3+"DHMSM");
		DHMSMCheckBox = (JCheckBox) DIHelper.getInstance().getLocalProp(ConstantsTAP.FUNCTIONAL_AREA_CHECKBOX_4+"DHMSM");
		Vector<String> capabilities = new Vector<String>();
		SourceSelectPanel sourcePanel = (SourceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.DHMSM_CAPABILITY_SELECT_PANEL);
		String sparqlQuery="";
		if (HSDCheckBox.isSelected())
		{
			sparqlQuery=DIHelper.getInstance().getProperty(ConstantsTAP.SOURCE_SELECT_REPORT_QUERY_HSD);
			capabilities.addAll(getCapabilityList(sparqlQuery));
		}
		if (HSSCheckBox.isSelected())
		{
			sparqlQuery=DIHelper.getInstance().getProperty(ConstantsTAP.SOURCE_SELECT_REPORT_QUERY_HSS);
			capabilities.addAll(getCapabilityList(sparqlQuery));
		}
		if (FHPCheckBox.isSelected())
		{
			sparqlQuery=DIHelper.getInstance().getProperty(ConstantsTAP.SOURCE_SELECT_REPORT_QUERY_FHP);
			capabilities.addAll(getCapabilityList(sparqlQuery));
		}
		if (DHMSMCheckBox.isSelected())
		{
			sparqlQuery=DIHelper.getInstance().getProperty(ConstantsTAP.SOURCE_SELECT_REPORT_QUERY_DHMSM);
			capabilities.addAll(getCapabilityList(sparqlQuery));
		}
		//unselect all
		Enumeration<String> enumKey = sourcePanel.checkBoxHash.keys();
		while(enumKey.hasMoreElements()) {
		    String key = enumKey.nextElement();
			JCheckBox checkBox = (JCheckBox) sourcePanel.checkBoxHash.get(key);
			checkBox.setSelected(false);

		}
		for (int i = 0; i< capabilities.size(); i++)
		{
			JCheckBox checkBox = (JCheckBox) sourcePanel.checkBoxHash.get(capabilities.get(i));
			checkBox.setSelected(true);
		}
	}
		
	/**
	 * Gets the list of all capabilities for a selected functional area
	 * @param sparqlQuery 		String containing the query to get all capabilities for a selected functional area
	 * @return capabilities		Vector<String> containing list of all capabilities for a selected functional area
	 */
	public Vector<String> getCapabilityList(String sparqlQuery)
	{
		Vector<String> capabilities = new Vector<String>();
		if(sparqlQuery==null)
			return capabilities;
		Hashtable paramTable = new Hashtable();
		String entityNS = DIHelper.getInstance().getProperty("Capability"+Constants.CLASS);
		paramTable.put(Constants.ENTITY, entityNS );
		sparqlQuery = Utility.fillParam(sparqlQuery, paramTable);	
		capabilities = engine.getEntityOfType(sparqlQuery);
		Hashtable paramHash = Utility.getInstanceNameViaQuery(capabilities);
		Set nameC = paramHash.keySet();
		capabilities = new Vector(nameC);
		return capabilities;
	}

	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
