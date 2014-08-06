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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.teamdev.jxbrowser.chromium.BrowserFunction;
import com.teamdev.jxbrowser.chromium.JSValue;

import prerna.rdf.engine.api.IEngine;


/**
 * An abstract browser class for SPARQL functions.
 */
public abstract class AbstractBrowserSPARQLFunction implements BrowserFunction {

	static final Logger logger = LogManager.getLogger(AbstractBrowserSPARQLFunction.class.getName());
	IEngine engine;
	
	/**
	 * Method invoke.  Overrides the invoke method from BrowserFunction.
	 * @param arg0 Object[]
	
	 * @return Object */
	@Override
	public abstract JSValue invoke(JSValue... arg0);
	
	/**
	 * Method setEngine.  Sets the local engine to the IEngine parameter.
	 * @param engine IEngine - The engine that the listener will access.
	 */
	public void setEngine(IEngine engine){
		this.engine = engine;
	}
}
