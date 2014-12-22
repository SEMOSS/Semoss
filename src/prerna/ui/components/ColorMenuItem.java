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

import javax.swing.JMenuItem;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;


/**
 * This is the abstract base class that executes specific queries and paints the item menu.
 */
public class ColorMenuItem extends JMenuItem{
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	String color = null;
	static final Logger logger = LogManager.getLogger(ColorMenuItem.class.getName());

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
			pickedVertex[vertIndex].setColor((Color)DIHelper.getInstance().getLocalProp(color));
			tcst.addColor(""+pickedVertex[vertIndex].getProperty(Constants.VERTEX_NAME), color);
		}
		GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();

		playSheet.repaint();
	}
}
