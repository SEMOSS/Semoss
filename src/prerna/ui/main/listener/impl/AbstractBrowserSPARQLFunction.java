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
