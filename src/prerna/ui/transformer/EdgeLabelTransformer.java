/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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
package prerna.ui.transformer;

import java.util.Collections;
import java.util.Vector;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSEdge;
import prerna.ui.components.ControlData;
import prerna.util.Constants;
import prerna.util.PropComparator;

/**
 * Transforms the property edge label on the graph.
 */
public class EdgeLabelTransformer implements Transformer <SEMOSSEdge, String> {
	
	static final Logger logger = LogManager.getLogger(EdgeLabelTransformer.class.getName());
	ControlData data = null;
	
	/**
	 * Constructor for EdgeLabelTransformer.
	 * @param data ControlData
	 */
	public EdgeLabelTransformer(ControlData data)
	{
		this.data = data;
	}

	/**
	 * Method transform.  Get the DI Helper to find what is needed to get for vertex
	 * @param arg0 DBCMEdge - The edge of which this returns the properties.
	
	 * @return String - The name of the property. */
	@Override
	public String transform(SEMOSSEdge arg0) {	
		String propName = "";
		//(String)arg0.getProperty(Constants.VERTEX_NAME);

		Vector props = this.data.getSelectedProperties(arg0.getProperty(Constants.EDGE_TYPE)+"");
		if(props != null && props.size() > 0)
		{
			propName = "<html>";
			//want to order the props so that it is always in the order name, type, uri, then the other properties
			Collections.sort(props, new PropComparator());
			for(int propIndex=0;propIndex < props.size();propIndex++){
				if(propIndex!=0) propName = propName + "<br>";
				propName = propName + "<!--"+arg0.getURI()+"-->";//Need this stupid comment to keep each html comment different. 
				//For some reason the transformer cannot handle text size changes if two labels are the same
				propName = propName + arg0.getProperty(props.elementAt(propIndex)+"");
			}
			propName = propName + "</html>";
		}
		//logger.debug("Prop Name " + propName);
		
		return propName;
	}
	
}
