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
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;

import javax.swing.JComboBox;
import javax.swing.JComponent;

import prerna.engine.api.IEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.rdf.RDFFileSesameEngine;
import prerna.error.EngineException;
import prerna.ui.comparison.specific.tap.GenericDBComparisonWriter;
import prerna.ui.components.api.IChakraListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class DatabaseComparisonListener implements IChakraListener
{
	
	@SuppressWarnings("unchecked")
	@Override
	public void actionPerformed(ActionEvent arg0)
	{
		// get selected values
		JComboBox<String> newDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.NEW_DB_COMBOBOX);
		String newDBName = newDBComboBox.getSelectedItem() + "";
		
		JComboBox<String> oldDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.OLD_DB_COMBOBOX);
		String oldDBName = oldDBComboBox.getSelectedItem() + "";
		
		// get associated engines
		IEngine newDB = (IEngine) DIHelper.getInstance().getLocalProp(newDBName);
		IEngine oldDB = (IEngine) DIHelper.getInstance().getLocalProp(oldDBName);
		
		RDFFileSesameEngine newMetaDB = ((AbstractEngine) newDB).getBaseDataEngine();
		RDFFileSesameEngine oldMetaDB = ((AbstractEngine) oldDB).getBaseDataEngine();
		
		try
		{
			GenericDBComparisonWriter comparisonWriter = new GenericDBComparisonWriter(newDB, oldDB, newMetaDB, oldMetaDB);
			comparisonWriter.runAllInstanceTests();
			comparisonWriter.runAllMetaTests();
			comparisonWriter.writeWB();
			
			Utility.showMessage("All tests are finished.");
		} catch (EngineException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public void setView(JComponent view)
	{
		// TODO Auto-generated method stub
		
	}
	
}
