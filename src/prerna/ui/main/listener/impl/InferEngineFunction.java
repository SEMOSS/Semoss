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

import prerna.rdf.engine.impl.BigDataEngine;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.JSValue;

/**
 */
public class InferEngineFunction extends AbstractBrowserSPARQLFunction {
	
	/**
	 * Method invoke.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public JSValue invoke(JSValue... arg0) {
		if(engine instanceof BigDataEngine){
			//((BigDataEngine)engine).commit();
			((BigDataEngine)engine).infer();
		}
		Hashtable retHash = new Hashtable();
		retHash.put("success", true);
		Gson gson = new Gson();        
		return JSValue.create(gson.toJson(retHash));
	}
	
}
