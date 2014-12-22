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

import prerna.om.SEMOSSVertex;

/**
 */
public class VertexStrokeTransformer implements Transformer <SEMOSSVertex, Stroke> {
	
	
	Hashtable <String, SEMOSSVertex> vertices = null;
	
	/**
	 * Constructor for VertexStrokeTransformer.
	 */
	public VertexStrokeTransformer()
	{
		
	}
	
	/**
	 * Method setEdges.
	 * @param edges Hashtable<String,DBCMVertex>
	 */
	public void setEdges(Hashtable <String, SEMOSSVertex> edges)
	{
		this.vertices = vertices;
	}
	

	/**
	 * Method transform.
	 * @param vertex DBCMVertex
	
	 * @return Stroke */
	@Override
	public Stroke transform(SEMOSSVertex vertex)
	{
		
		
		Stroke retStroke = new BasicStroke(1.0f);
		try
		{	
			retStroke = new BasicStroke(0f);
                

		}
		catch(RuntimeException ex) {
			//ignore
			System.out.println("ignored");
		}
		return retStroke;
	}
}
