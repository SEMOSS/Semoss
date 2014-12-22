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
package prerna.ui.components;

import java.awt.Color;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.DIHelper;


/**
 * This class is used primarily for vertex filtering.
 */
public class VertexColorShapeData {
	
	// need to have vertex and type information
	// everytime a vertex is added here
	// need to figure out a type so that it can show
	// the types are not needed after or may be it is
	// we need a structure which keeps types with vector
	// the vector will have all of the vertex specific to the type
	// additionally, there needs to be another structure so that when I select or deselect something
	// it marks it on the matrix
	// need to come back and solve this one
	Hashtable <String, Vector> typeHash = new Hashtable<String, Vector >();
	
	public Object [] scColumnNames = {"Node", "Instance", "Shape", "Color"};

	public Class[] classNames = {Object.class, Object.class, Object.class};

	// Table rows for vertices
	String [][] shapeColorRows = null;
	
	int count = 0;
	static final Logger logger = LogManager.getLogger(VertexColorShapeData.class.getName());
	
	/**
	 * Sets the type hashtable.
	 * @param typeHash 	Hashtable of the type <String, Vector>.
	 */
	public void setTypeHash(Hashtable <String, Vector> typeHash)
	{
		this.typeHash = typeHash;
	}
	
	/**
	 * Sets the count.
	 * @param count 	Integer.
	 */
	public void setCount(int count)
	{
		this.count = count;
	}
	
	/**
	 * Gets cell value at a particular row and column index.
	 * @param row 		Row index.
	 * @param column 	Column index.
	
	 * @return Object 	Cell value. */
	public Object getValueAt(int row, int column)
	{
		return shapeColorRows[row][column];
	}
		
	/**
	 * Gets the number of rows.
	
	 * @return int	Number of rows. */
	public int getNumRows()
	{
		// use this call to convert the thing to array
		return count;
	}
	
	/**
	 * Fills the rows of vertex colors and shapes based on the vertex name and type.
	
	 * @return String[][]		Array containing information about vertex colors and shapes. */
	public String [][] fillRows()
	{
			logger.info("Fill Rows Called >>>>>>>>>>>>>>" + count);
			shapeColorRows = new String [count][scColumnNames.length];
			
			Enumeration <String> keys = typeHash.keys();
			int rowCount = 0;
			int keyCount = 0;
			while(keys.hasMoreElements())
			{
				String vertType = keys.nextElement();
				Vector <SEMOSSVertex> vertVector = typeHash.get(vertType);
				
				for(int vertIndex = 0;vertIndex < vertVector.size();vertIndex++)
				{
					SEMOSSVertex vert = vertVector.elementAt(vertIndex);
					String vertName = (String)vert.getProperty(Constants.VERTEX_NAME);

					if(vertIndex == 0)
					{
						shapeColorRows[rowCount][0] = vertType;
						shapeColorRows[rowCount][1] = "Select All";					
						shapeColorRows[rowCount][2] = "TBD"; //TypeColorShapeTable.getInstance().getShapeAsString(vertName);
						shapeColorRows[rowCount][3] = "TBD"; //TypeColorShapeTable.getInstance().getColorAsString(vertName);;
						rowCount++;
					}	
					shapeColorRows[rowCount][1] = vertName;
					//shapeColorRows[rowCount][2] = "TBD";
					//shapeColorRows[rowCount][3] = "TBD";
					shapeColorRows[rowCount][2] = TypeColorShapeTable.getInstance().getShapeAsString(vertName);
					shapeColorRows[rowCount][3] = TypeColorShapeTable.getInstance().getColorAsString(vertName);;
					
					logger.debug(">>> " + vertType + "<<>>" + vertName);
					
					// do the logic of already selected color and shape here
					rowCount++;
				}	
				keyCount++;
			}
		logger.info("Fill Rows Complete");
		return shapeColorRows;
	}
	
	// uses URI
	/**
	 * Sets the cell value at a particular row and column index.
	 * @param value 	Cell value.
	 * @param row 		Row index (int).
	 * @param column 	Column index (int).
	 */
	public void setValueAt(Object value, int row, int column)
	{
		// almost always this is the case of setting the color or shape
		
		// if column = 2 = shape
		
		//get the second column to see if its empty
		shapeColorRows[row][column] = value+ "";
		String nodeType = shapeColorRows[row][0];
		
		if(nodeType != null && nodeType.length() > 0)
		{
			Vector <SEMOSSVertex> vertVector = typeHash.get(nodeType);	
			// get this nodetype and make all of them negative				
			for(int vertIndex = 0;vertIndex < vertVector.size();vertIndex++)
			{
				SEMOSSVertex vert = vertVector.elementAt(vertIndex);
				String vertName = (String)vert.getProperty(Constants.VERTEX_NAME);
				
				shapeColorRows[row + vertIndex+1][column] = value+"";
				logger.debug("Creating shape " + value + "   For Vertex Name " + vertName);
				addColorShape(column, vert, value+"");
			}		
			// also add the node type to the store
			//addColorShape(column, nodeType, value+"");
		}
		else
		{
			// only that node
			//see what the first type above the selected row is (it will be the type of that node)
			int i = 0;
			while(nodeType == null){
				i++;
				nodeType = shapeColorRows[row - i][0];
			}
			//get that node
			Vector <SEMOSSVertex> vertVector = typeHash.get(nodeType);	
			SEMOSSVertex vert = vertVector.elementAt(i-1);
			shapeColorRows[row][column] = value+"";
			String vertName = shapeColorRows[row][1];
			logger.debug("Creating shape " + value + "   For Vertex Name " + vertName);
			addColorShape(column, vert, value+"");
		}
	}
	
	/**
	 * Adds the colors and shapes to the table.
	 * @param column 	Column index.
	 * @param vertName 	Name of the vertex, in string form.
	 * @param value 	Value associated with the vertex name, in string form.
	 */
	private void addColorShape(int column, SEMOSSVertex vert, String value)
	{
		if(column == 2) // this is shape
			TypeColorShapeTable.getInstance().addShape(vert.getProperty(Constants.VERTEX_NAME)+"", value);
		else if (column == 3){
			vert.setColor((Color)DIHelper.getInstance().getLocalProp(value));
			TypeColorShapeTable.getInstance().addColor(vert.getProperty(Constants.VERTEX_NAME)+"", value);
		}
	}
}
