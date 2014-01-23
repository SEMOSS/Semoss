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
package prerna.ui.transformer;

import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;

/**
 * Transforms the size and shape of selected nodes.
 */
public class VertexShapeTransformer implements Transformer <SEMOSSVertex, Shape> {
	
	Hashtable<String, Double> vertSizeHash;
	Hashtable<String, Double> vertSelectionHash;
	Logger logger = Logger.getLogger(getClass());
	double maxSize = 100.0;
	double minSize = 0.0;
	double currentDefaultScale;
	double initialDefaultScale=1;
	double sizeDelta = .5;
	
	/**
	 * Constructor for VertexShapeTransformer.
	 */
	public VertexShapeTransformer()
	{
		vertSizeHash = new Hashtable();
		vertSelectionHash = new Hashtable();
		currentDefaultScale = initialDefaultScale;		
	}
	
	/**
	 * Method getDefaultScale. Gets the default scale 
	 * @param nodeURI String
	 */
	public double getDefaultScale()
	{
		return currentDefaultScale;
	}
	
	/**
	 * Method setVertexSizeHash.  Sets the local vertex size hash 
	 * @param Hashtable<String, Double> vertSizeHash 
	 */
	public void setVertexSizeHash(Hashtable<String, Double> vertSizeHash)
	{
		this.vertSizeHash= vertSizeHash;
	}
	
	/**
	 * Method setSelected. Keeps track of the changes that need to be undone.
	 * @param nodeURI String
	 */
	public void setSelected(String nodeURI){
		double selectedSize = sizeDelta;
		if(vertSelectionHash.containsKey(nodeURI))
			selectedSize = vertSelectionHash.get(nodeURI) - sizeDelta;
		vertSelectionHash.put(nodeURI, selectedSize);
	}
	
	/**
	 * Method emptySelected.  Clears the selection hashtable of all selections.
	 */
	public void emptySelected(){
		vertSelectionHash.clear();
	}

	/**
	 * Method increaseSize.  Increases the size of a selected node.
	 * @param nodeURI String - the URI of the node to be increased.
	 */
	public void increaseSize(String nodeURI){
		if(vertSizeHash.containsKey(nodeURI)){
			double size = vertSizeHash.get(nodeURI);
			if(size<maxSize)
				size = size+sizeDelta;
			vertSizeHash.put(nodeURI, size);
		}
		else{
			double size = currentDefaultScale;
			if(size<maxSize)
				size = size +sizeDelta;
			vertSizeHash.put(nodeURI, size);
		}
	}

	/**
	 * Method decreaseSize.  Decreases the size of a selected node.
	 * @param nodeURI String - the URI of the node to be decreased.
	 */
	public void decreaseSize(String nodeURI){
		if(vertSizeHash.containsKey(nodeURI)){
			double size = vertSizeHash.get(nodeURI);
			if(size>minSize)
				size = size-sizeDelta;
			vertSizeHash.put(nodeURI, size);
		}
		else{
			double size = currentDefaultScale;
			if(size>minSize)
				size = size -sizeDelta;
			vertSizeHash.put(nodeURI, size);
		}
	}

	/**
	 * Method increaseSize.  Increases the size of all the nodes on a graph.
	 */
	public void increaseSize(){
		//Increase everything that does not have a size specified
		if(currentDefaultScale<maxSize)
			currentDefaultScale = currentDefaultScale + sizeDelta;
		//Increase everything with a size specified
		Iterator vertURIs = vertSizeHash.keySet().iterator();
		while(vertURIs.hasNext()){
			String vertURI = (String) vertURIs.next();
			double size = vertSizeHash.get(vertURI);
			if(size<maxSize)
				size = size+sizeDelta;
			vertSizeHash.put(vertURI, size);
		}
	}

	/**
	 * Method decreaseSize.  Decreases the size of all the nodes on a graph.
	 */
	public void decreaseSize(){
		//Decrease everything that does not have a size specified
		if(currentDefaultScale>minSize)
			currentDefaultScale = currentDefaultScale - sizeDelta;
		//Decrease everything with a size specified
		Iterator vertURIs = vertSizeHash.keySet().iterator();
		while(vertURIs.hasNext()){
			String vertURI = (String) vertURIs.next();
			double size = vertSizeHash.get(vertURI);
			if(size>minSize)
				size = size-sizeDelta;
			vertSizeHash.put(vertURI, size);
		}
	}
	
	/**
	 * Method transform.  Get the DI Helper to find what is needed to get for vertex
	 * @param arg0 DBCMVertex - The edge of which this returns the properties.
	
	 * @return Shape - The name of the new shape. */
	@Override
	public Shape transform(SEMOSSVertex arg0) {
		String URI = (String)arg0.getProperty(Constants.URI);

		String propType = (String)arg0.getProperty(Constants.VERTEX_TYPE);
		String vertName = (String)arg0.getProperty(Constants.VERTEX_NAME);
		
		Shape type = null;
		type = TypeColorShapeTable.getInstance().getShape(propType, vertName);
		
		//only need to tranform if uri is contained in hash or current size does not equal default size
		if(vertSizeHash.containsKey(URI)||currentDefaultScale != initialDefaultScale || vertSelectionHash.containsKey(URI)){
			double customScale = currentDefaultScale;
			if(vertSizeHash.containsKey(URI))
				customScale = vertSizeHash.get(arg0.getURI());
			if(vertSelectionHash.containsKey(URI))
				customScale = customScale + vertSelectionHash.get(URI);
			type = AffineTransform.getScaleInstance(customScale, customScale).createTransformedShape(type);
		}
		
        return type;
	}
}
