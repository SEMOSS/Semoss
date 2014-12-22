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

import java.awt.BasicStroke;
import java.awt.Stroke;
import java.util.Hashtable;

import org.apache.commons.collections15.Transformer;

import prerna.om.SEMOSSEdge;
import prerna.util.Constants;

/**
 */
public class EdgeStrokeTransformer implements Transformer <SEMOSSEdge, Stroke> {
	
	
	Hashtable edges = null;
	
	/**
	 * Constructor for EdgeStrokeTransformer.
	 */
	public EdgeStrokeTransformer()
	{
		
	}
	
	/**
	 * Method setEdges.
	 * @param edges Hashtable
	 */
	public void setEdges(Hashtable edges)
	{
		this.edges = edges;
	}
	
	/**
	 * Method getEdges.
	
	 * @return Hashtable */
	public Hashtable getEdges(){
		return edges;
	}
	
	public boolean isDash = false;
	
	public void setDash(boolean isDash){
		this.isDash = isDash;
	}

	/**
	 * Method transform.
	 * @param edge DBCMEdge
	
	 * @return Stroke */
	@Override
	public Stroke transform(SEMOSSEdge edge)
	{
		
		float selectedFontFloat =3.0f;
		float unselectedFontFloat =0.1f;

		float standardFontFloat = 0.3f;

		Stroke retStroke = new BasicStroke(1.0f);
		try	{	
			if (edges != null) {
				if (edges.containsKey(edge.getProperty(Constants.URI))) {
					Object val = edges.get(edge.getProperty(Constants.URI));
					try {
						double valDouble = (Double) val;
						float valFloat = (float) valDouble;
						float newFontFloat = selectedFontFloat * valFloat;
						retStroke = new BasicStroke(newFontFloat, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f);
					} catch (RuntimeException e) {
						//TODO: Specify exception(s)
						retStroke = new BasicStroke(selectedFontFloat, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f);
						// logger.info("shortest path edges");
					}
				} else {
					if(isDash) {
						retStroke = new BasicStroke(standardFontFloat, BasicStroke.CAP_BUTT, BasicStroke.JOIN_ROUND, 10.0f, new float[]{10.0f}, 0.0f);
					} else {
						retStroke = new BasicStroke(unselectedFontFloat);
					}
				}
			} else {
				retStroke = new BasicStroke(standardFontFloat, BasicStroke.CAP_BUTT, BasicStroke.JOIN_ROUND);
			}
		}
		catch(RuntimeException ex) {
			//TODO: Specify exception(s) and action
			//ignore
			System.out.println("ignored");
		}
		return retStroke;
	}
}
