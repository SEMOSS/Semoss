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
