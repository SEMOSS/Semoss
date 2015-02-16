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

import java.awt.Font;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSEdge;
import prerna.util.Constants;

/**
 * Transforms the font label on an edge in the graph.
 */
public class EdgeLabelFontTransformer implements Transformer <SEMOSSEdge, Font> {
	

	Hashtable <String, String> edgeURI2Show = null;
	static final Logger logger = LogManager.getLogger(EdgeLabelFontTransformer.class.getName());
	int initialDefaultSize=8;
	int currentDefaultSize;
	int maxSize = 55;
	int minSize = 0;
	//This stores all font size data about the nodes.  Different than verticeURI2Show because need to remember size information when vertex label is unhidden
	Hashtable<String, Integer> edgeSizeData;
	

	/**
	 * Constructor for EdgeLabelFontTransformer.
	 */
	public EdgeLabelFontTransformer()
	{
		edgeSizeData = new Hashtable();
		currentDefaultSize = initialDefaultSize;
	}
	
	/**
	 * Method getFontSizeData.  Gets the hashtable of node font size data.
	
	 * @return Hashtable */
	public Hashtable getFontSizeData(){
		return edgeSizeData;
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
		this.edgeURI2Show = verticeURI2Show;
	}

	/**
	 * Method clearSizeData.  Clears all the font size data from the size hashtable.
	 */
	public void clearSizeData()
	{
		edgeSizeData.clear();
		currentDefaultSize = initialDefaultSize;
	}
	
	/**
	 * Method increaseFontSize.  Increases the font size for all the nodes in the graph.
	 */
	public void increaseFontSize(){
		if(currentDefaultSize<maxSize)
			currentDefaultSize++;
		Iterator vertURIs = edgeSizeData.keySet().iterator();
		while(vertURIs.hasNext()){
			String vertURI = (String) vertURIs.next();
			int size = edgeSizeData.get(vertURI);
			if(size<maxSize)
				size = size+1;
			edgeSizeData.put(vertURI, size);
		}
	}

	/**
	 * Method decreaseFontSize. Decreases the font size for all the nodes in the graph.
	 */
	public void decreaseFontSize(){
		if(currentDefaultSize>minSize)
			currentDefaultSize--;
		Iterator vertURIs = edgeSizeData.keySet().iterator();
		while(vertURIs.hasNext()){
			String vertURI = (String) vertURIs.next();
			int size = edgeSizeData.get(vertURI);
			if(size>minSize)
				size = size-1;
			edgeSizeData.put(vertURI, size);
		}
	}

	/**
	 * Method increaseFontSize.  Increases the font size of a selected node on the graph.
	 * @param nodeURI String - the node URI of the selected node.
	 */
	public void increaseFontSize(String nodeURI){
		if(edgeSizeData.containsKey(nodeURI)){
			int size = edgeSizeData.get(nodeURI);
			if(size<maxSize)
				size = size+1;
			edgeSizeData.put(nodeURI, size);
		}
		else{
			int size = currentDefaultSize;
			if(size<maxSize)
				size = size +1;
			edgeSizeData.put(nodeURI, size);
		}
	}

	/**
	 * Method decreaseFontSize.  Decreases the font size of a selected node on the graph.
	 * @param nodeURI String - the node URI of the selected node.
	 */
	public void decreaseFontSize(String nodeURI){
		if(edgeSizeData.containsKey(nodeURI)){
			int size = edgeSizeData.get(nodeURI);
			if(size>minSize)
				size = size-1;
			edgeSizeData.put(nodeURI, size);
		}
		else{
			int size = currentDefaultSize;
			if(size>minSize)
				size = size -1;
			edgeSizeData.put(nodeURI, size);
		}
	}
	

	/**
	 * Method transform.  Transforms the label on an edge in the graph
	 * @param arg0 DBCMEdge - the edge to be transformed
	
	 * @return Font - the font of the edge*/
	@Override
	public Font transform(SEMOSSEdge arg0)
	{
		int customSize = currentDefaultSize;
		if(edgeSizeData.containsKey(arg0.getURI()))
			customSize = edgeSizeData.get(arg0.getURI());
		Font font = new Font("Plain", Font.PLAIN, customSize);

		if(edgeURI2Show != null)
		{
			String URI = (String)arg0.getProperty(Constants.URI);
			logger.debug("URI " + URI);
			if(edgeURI2Show.containsKey(URI))
			{
				font = new Font("Plain", Font.PLAIN, customSize);
			}
			else font = new Font("Plain", Font.PLAIN, 0);
		}
		return font;
	}
}
