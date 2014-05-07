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
package prerna.ui.main.listener.impl;

import java.util.Hashtable;

import prerna.ui.components.playsheets.GraphPlaySheet;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.BrowserFunction;
import com.teamdev.jxbrowser.chromium.JSValue;

/**
 * Refreshes the view of the graph play sheet.
 */
public class RefreshPlaysheetFunction implements BrowserFunction {
	
	GraphPlaySheet gps;
	
	/**
	 * Method setGps.  Sets the graph play sheet that the listener will access.
	 * @param gps GraphPlaySheet
	 */
	public void setGps(GraphPlaySheet gps) {
		this.gps = gps;
	}

	/**
	 * Method invoke.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public JSValue invoke(JSValue... arg0) {
		gps.refineView();

		Hashtable retHash = new Hashtable();
		retHash.put("success", true);

		Gson gson = new Gson();
        
		return JSValue.create(gson.toJson(retHash));
	}
	
}
