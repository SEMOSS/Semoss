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
package prerna.ui.transformer;

import java.util.Vector;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.ControlData;
import prerna.util.Constants;

/**
 * Transforms what is displayed on the tooltip when a vertex/node is selected on a graph.
 */
public class VertexTooltipTransformer implements Transformer <SEMOSSVertex, String> {
	
	static final Logger logger = LogManager.getLogger(VertexTooltipTransformer.class.getName());	
	ControlData data = null;
	
	/**
	 * Constructor for VertexTooltipTransformer.
	 * @param data ControlData
	 */
	public VertexTooltipTransformer(ControlData data)
	{
		this.data = data;
	}
	
	
	/**
	 * Method transform.  Get the DI Helper to find what is needed to get for vertex
	 * @param arg0 DBCMVertex - The edge of which this returns the properties.
	
	 * @return String - The name of the property. */
	@Override
	public String transform(SEMOSSVertex arg0) {	
		String propName = "";//(String)arg0.getProperty(Constants.VERTEX_NAME);

		Vector props = this.data.getSelectedPropertiesTT(arg0.getProperty(Constants.VERTEX_TYPE)+"");
		if(props != null && props.size() > 0)
		{
			propName = propName + arg0.getProperty(props.elementAt(0)+"");
			for(int propIndex=1;propIndex < props.size();propIndex++){
				String prop = props.elementAt(propIndex)+"";
				propName = propName + "<br>";
				//only add the label on the property if it is not one of the main three
				if(!prop.equals(Constants.VERTEX_NAME)&&!prop.equals(Constants.EDGE_NAME)&&!prop.equals(Constants.VERTEX_TYPE)&&!prop.equals(Constants.EDGE_TYPE)&&!prop.equals(Constants.URI))
					propName = propName + prop+": ";
				propName = propName + arg0.getProperty(props.elementAt(propIndex)+"");
			}
		}
		//logger.debug("Prop Name " + propName);
		
		if(propName.equals(""))
			return null;		
		

		propName = "<html><body style=\"border:0px solid white; box-shadow:1px 1px 1px #000; padding:2px; background-color:white;\">" +
				"<font size=\"3\" color=\"black\"><i>" + propName + "</i></font></body></html>";
		return propName;
	}
}
