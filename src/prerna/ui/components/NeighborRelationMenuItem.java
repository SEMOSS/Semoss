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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;


/**
 * This class is used to create a menu item for composing relationships for the neighborhood.
 */
public class NeighborRelationMenuItem extends JMenuItem{

	String predicateURI = null;
	String name = null;
	
	static final Logger logger = LogManager.getLogger(NeighborRelationMenuItem.class.getName());

	/**
	 * Constructor for NeighborRelationMenuItem.
	 * @param name String
	 * @param predicateURI String
	 */
	public NeighborRelationMenuItem(String name, String predicateURI)
	{
		super(name);
		this.name = name;
		this.predicateURI = predicateURI;
	}
	
	/**
	 * Composes the query and paints the neighborhood based on predicate URIs.
	 */
	public void paintNeighborhood()
	{
		IPlaySheet ps = QuestionPlaySheetStore.getInstance().getActiveSheet();
		// need to find a way to add the relationship here
		String predURI = DIHelper.getInstance().getProperty(Constants.PREDICATE_URI);
		predURI += ";" + name;
		DIHelper.getInstance().putProperty(Constants.PREDICATE_URI, predURI);		
		ps.refineView();
	}
	
	
}
