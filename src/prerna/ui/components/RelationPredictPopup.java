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

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JList;
import javax.swing.JMenu;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.NeighborRelationMenuListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class creates a popup menu to visualize possible relationships given two queries.
 */
public class RelationPredictPopup extends JMenu implements MouseListener{
	
	// given 2 queries it will visualize the relationships possible
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(RelationPredictPopup.class.getName());

	// both of them bind
	String mainQuery = null;
	
	// subject bind but not object, object as label
	String mainQuery2 = null;
	
	//both subject and object as label
	String mainQuery3 = null;
	String neighborQuery = null;
	String altQuery = null;
	String altMainQuery = null;
	String altQuery2 = null;
	
	boolean instance = false;
	// core class for neighbor hoods etc.
	/**
	 * Constructor for RelationPredictPopup.
	 * @param name String
	 * @param ps IPlaySheet
	 * @param pickedVertex DBCMVertex[]
	 */
	public RelationPredictPopup(String name, IPlaySheet ps, SEMOSSVertex [] pickedVertex)
	{
		super(name);
		// need to get this to read from popup menu
		this.ps = ps;
		this.pickedVertex = pickedVertex;
		this.mainQuery = Constants.NEIGHBORHOOD_PREDICATE_FINDER_QUERY;
		this.mainQuery2 = Constants.NEIGHBORHOOD_PREDICATE_ALT2_FINDER_QUERY;
		this.mainQuery3 = Constants.NEIGHBORHOOD_PREDICATE_ALT3_FINDER_QUERY;
		addMouseListener(this);
	}
	
	/**
	 * Executes the query and adds all the relationships.
	 */
	public void addRelations()
	{

		// the relationship needs to have the subject - selected vertex
		// need to add the relationship to the relationship URI
		// and the predicate selected
		// the listener should then trigger the graph play sheet possibly
		// and for each relationship add the listener
		Hashtable<String, String> hash = new Hashtable<String, String>();
		String ignoreURI = DIHelper.getInstance().getProperty(Constants.IGNORE_URI);
		// there should exactly be 2 vertices
		// we try to find the predicate based on both subject and object
		SEMOSSVertex vert1 = pickedVertex[0];
		String uri = vert1.getURI();
		SEMOSSVertex vert2 = pickedVertex[1];
		String uri2 = vert2.getURI();
		
		Pattern pattern = Pattern.compile("\\s");
		Matcher matcher = pattern.matcher(uri);
		boolean found1 = matcher.find();
		matcher = pattern.matcher(uri2);
		boolean found2 = matcher.find();
		
		String subject = uri;
		String object = uri2;
		String query = DIHelper.getInstance().getProperty(mainQuery);
		
		if(found1 && uri.contains("/"))
			subject = "\"" + Utility.getInstanceName(uri) + "\"";	
		else if(uri.contains("/"))
			subject = "<" + uri + ">";
		if(found2 && uri2.contains("/"))
			object = "\"" + Utility.getInstanceName(uri2) + "\"";	
		else if(uri2.contains("/"))
			object = "<" + uri2 + ">";
		
		hash.put(Constants.SUBJECT, subject);
		hash.put(Constants.OBJECT, object);
		
		if(found1 && uri.contains("/"))
		{
			query = DIHelper.getInstance().getProperty(mainQuery2);
			if(found2 && uri2.contains("/"))
				query = DIHelper.getInstance().getProperty(mainQuery3);
		}
		
		String query1 = Utility.fillParam(query, hash);

		if(found2 && uri.contains("/"))
		{
			query = DIHelper.getInstance().getProperty(mainQuery2);
			if(found1 && uri2.contains("/"))
				query = DIHelper.getInstance().getProperty(mainQuery3);
		}
			

		hash.put(Constants.SUBJECT, object);
		hash.put(Constants.OBJECT, subject);
		
		String query2 = Utility.fillParam(query, hash);
		
		
		JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		// get the selected repository
		Object [] repos = (Object [])list.getSelectedValues();
		
		// I am only going to get one repository
		// hopefully they have selected one :)
		String repo = repos[0] +"";
		
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo);
		logger.debug("Found the engine for repository   " + repo);

		// run the query
		SesameJenaSelectWrapper sjw = new SesameJenaSelectWrapper();
		sjw.setEngine(engine);
		sjw.setEngineType(engine.getEngineType());
		sjw.setQuery(query1);
		sjw.executeQuery();
		
		logger.debug("Executed Query");
		logger.info("Executing query " + query1);
		
		String [] vars = sjw.getVariables();
		while(sjw.hasNext())
		{
			SesameJenaSelectStatement stmt = sjw.next();
			// only one variable
			
			String predName = stmt.getRawVar(vars[0]) + "";
			
			if(predName.length() > 0 && !Utility.checkPatternInString(ignoreURI, predName))
			{
				NeighborRelationMenuItem nItem = new NeighborRelationMenuItem(predName, predName);
				nItem.addActionListener(NeighborRelationMenuListener.getInstance());
				add(nItem);
			}
		}			
		
		// do it the other way now
		sjw.setQuery(query2);
		sjw.executeQuery();
		logger.info("Executing query " + query2);
		
		logger.debug("Executed Query");
		while(sjw.hasNext())
		{
			SesameJenaSelectStatement stmt = sjw.next();
			// only one variable
			
			String predName = stmt.getRawVar(vars[0]) + "";
			
			if(predName.length() > 0 && !Utility.checkPatternInString(ignoreURI, predName))
			{
				NeighborRelationMenuItem nItem = new NeighborRelationMenuItem(predName, predName);
				nItem.addActionListener(NeighborRelationMenuListener.getInstance());
				add(nItem);
			}
		}			
	}

	/**
	 * Invoked when the mouse button has been clicked (pressed and released) on a component.
	 * Adds relations once the mouse has been clicked.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseClicked(MouseEvent arg0) {
		logger.info("Mouse Entered and Clicked");
		addRelations();
	}

	/**
	 * Invoked when the mouse enters a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseEntered(MouseEvent arg0) {
		logger.info("Mouse Entered and Clicked");
	//	addRelations();
		
	}

	/**
	 * Invoked when the mouse exits a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseExited(MouseEvent arg0) {
		
	}

	/**
	 * Invoked when a mouse button has been pressed on a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mousePressed(MouseEvent arg0) {
		
	}

	/**
	 * Invoked when a mouse button has been released on a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseReleased(MouseEvent arg0) {
		
	}	
}
