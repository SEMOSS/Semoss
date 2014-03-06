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

import java.util.Hashtable;

import javax.swing.JDesktopPane;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.specific.tap.CapabilityFactSheet;
import prerna.ui.components.specific.tap.CapabilityFactSheetPerformer;
import prerna.ui.components.specific.tap.HealthGridSheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.ui.main.listener.impl.AbstractBrowserSPARQLFunction;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

import com.google.gson.Gson;

/**
 */
public class CapabilityFactSheetListener extends AbstractBrowserSPARQLFunction {
	
	CapabilityFactSheet cfs;

	/**
	 * Method setCapabilityFactSheet
	 * @param cfs CapabilityFactSheet that this listener acts on.
	 */
	public void setCapabilityFactSheet(CapabilityFactSheet cfs)
	{
		this.cfs = cfs;
	}
	
	/**
	 * Method invoke.
	 * @param arg0 Object[]
	 * @return Object 
	 */
	@Override
	public Object invoke(Object... arg0) {
		Gson gson = new Gson();
		String sysArrayString = (String) arg0[0];
		String[] sysArray = gson.fromJson(sysArrayString, String[].class);
		
		String capability = sysArray[0];
	//	String capability = "Access_a_Healthy_and_Fit_Force";
		System.out.println("Capability chosen is "+capability);

		//add in new capability
		
		cfs.processNewCapability(capability);

		return arg0;
	}
	
}
