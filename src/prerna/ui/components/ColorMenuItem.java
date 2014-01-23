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
package prerna.ui.components;

import javax.swing.JMenuItem;

import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.QuestionPlaySheetStore;


/**
 * This is the abstract base class that executes specific queries and paints the item menu.
 */
public class ColorMenuItem extends JMenuItem{
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	String color = null;
	Logger logger = Logger.getLogger(getClass());

	/**
	 * Constructor for ColorMenuItem.
	 * @param color String					Color.
	 * @param ps IPlaySheet	Playsheet.		Playsheet.
	 * @param pickedVertex DBCMVertex[]		Picked DBCM Vertex.
	 */
	public ColorMenuItem(String color, IPlaySheet ps, SEMOSSVertex [] pickedVertex)
	{
		super(color);
		this.ps = ps;
		this.color = color;
		this.pickedVertex = pickedVertex;
	}
	
	/**
	 * Paints the color onto the menu.
	 */
	public void paintColor()
	{
		TypeColorShapeTable tcst = TypeColorShapeTable.getInstance();
		for(int vertIndex = 0;vertIndex < pickedVertex.length;vertIndex++)
		{
			tcst.addColor(""+pickedVertex[vertIndex].getProperty(Constants.VERTEX_NAME), color);
		}
		GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();

		playSheet.repaint();
	}
}
