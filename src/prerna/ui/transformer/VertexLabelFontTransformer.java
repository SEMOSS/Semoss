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

import java.awt.Font;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.util.Constants;

/**
 * Transforms the font label on a node vertex in the graph.
 */
public class VertexLabelFontTransformer implements Transformer <SEMOSSVertex, Font> {	

	Hashtable <String, Object> verticeURI2Show = null;
	static final Logger logger = LogManager.getLogger(VertexLabelFontTransformer.class.getName());
	int initialDefaultSize=8;
	int currentDefaultSize;
	int maxSize = 55;
	int minSize = 0;
	//This stores all font size data about the nodes.  Different than verticeURI2Show because need to remember size information when vertex label is unhidden
	Hashtable<String, Integer> nodeSizeData;	

	/**
	 * Constructor for VertexLabelFontTransformer.
	 */
	public VertexLabelFontTransformer()
	{
		nodeSizeData = new Hashtable();
		currentDefaultSize = initialDefaultSize;
	}

	/**
	 * Method getVertHash.  Gets the hashtable of vertices and URIs.
	
	 * @return Hashtable<String,Object> */
	public Hashtable<String, Object> getVertHash()
	{
		return verticeURI2Show;
	}
	
	/**
	 * Method getFontSizeData.  Gets the hashtable of node font size data.
	
	 * @return Hashtable */
	public Hashtable getFontSizeData(){
		return nodeSizeData;
	}
	
	/**
	 * Method getCurrentFontSize.  Retrieves the current default font size for the nodes.
	
	 * @return int - the current font size.*/
	public int getCurrentFontSize(){
		return currentDefaultSize;
	}
	
	/**
	 * Method setVertHash.  Sets the hashtable of all the vertices to show on a graph.
	 * @param verticeURI2Show Hashtable
	 */
	public void setVertHash(Hashtable verticeURI2Show)
	{
		this.verticeURI2Show = verticeURI2Show;
	}

	/**
	 * Method clearSizeData.  Clears all the font size data from the size hashtable.
	 */
	public void clearSizeData()
	{
		nodeSizeData.clear();
		currentDefaultSize = initialDefaultSize;
	}
	
	/**
	 * Method increaseFontSize.  Increases the font size for all the nodes in the graph.
	 */
	public void increaseFontSize(){
		if(currentDefaultSize<maxSize)
			currentDefaultSize++;
		Iterator vertURIs = nodeSizeData.keySet().iterator();
		while(vertURIs.hasNext()){
			String vertURI = (String) vertURIs.next();
			int size = nodeSizeData.get(vertURI);
			if(size<maxSize)
				size = size+1;
			nodeSizeData.put(vertURI, size);
		}
	}

	/**
	 * Method decreaseFontSize. Decreases the font size for all the nodes in the graph.
	 */
	public void decreaseFontSize(){
		if(currentDefaultSize>minSize)
			currentDefaultSize--;
		Iterator vertURIs = nodeSizeData.keySet().iterator();
		while(vertURIs.hasNext()){
			String vertURI = (String) vertURIs.next();
			int size = nodeSizeData.get(vertURI);
			if(size>minSize)
				size = size-1;
			nodeSizeData.put(vertURI, size);
		}
	}

	/**
	 * Method increaseFontSize.  Increases the font size of a selected node on the graph.
	 * @param nodeURI String - the node URI of the selected node.
	 */
	public void increaseFontSize(String nodeURI){
		if(nodeSizeData.containsKey(nodeURI)){
			int size = nodeSizeData.get(nodeURI);
			if(size<maxSize)
				size = size+1;
			nodeSizeData.put(nodeURI, size);
		}
		else{
			int size = currentDefaultSize;
			if(size<maxSize)
				size = size +1;
			nodeSizeData.put(nodeURI, size);
		}
	}

	/**
	 * Method decreaseFontSize.  Decreases the font size of a selected node on the graph.
	 * @param nodeURI String - the node URI of the selected node.
	 */
	public void decreaseFontSize(String nodeURI){
		if(nodeSizeData.containsKey(nodeURI)){
			int size = nodeSizeData.get(nodeURI);
			if(size>minSize)
				size = size-1;
			nodeSizeData.put(nodeURI, size);
		}
		else{
			int size = currentDefaultSize;
			if(size>minSize)
				size = size -1;
			nodeSizeData.put(nodeURI, size);
		}
	}
	

	/**
	 * Method transform.  Transforms the label on a node vertex in the graph
	 * @param arg0 DBCMVertex - the vertex to be transformed
	
	 * @return Font - the font of the vertex*/
	@Override
	public Font transform(SEMOSSVertex arg0)
	{
		int customSize = currentDefaultSize;
		if(nodeSizeData.containsKey(arg0.getURI()))
			customSize = nodeSizeData.get(arg0.getURI());
		Font font = new Font("Plain", Font.PLAIN, customSize);

		if(verticeURI2Show != null)
		{
			String URI = (String)arg0.getProperty(Constants.URI);
			logger.debug("URI " + URI);
			if(verticeURI2Show.containsKey(URI))
			{
				font = new Font("Plain", Font.PLAIN, customSize);
			}
			else font = new Font("Plain", Font.PLAIN, 0);
		}
		return font;
	}
	
}
