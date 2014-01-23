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

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JList;
import javax.swing.JMenu;

import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.NeighborMenuListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

/**
 * This class creates a popup window for abstract relations.
 */
public abstract class AbstractRelationPopup extends JMenu implements MouseListener{
	
	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	Logger logger = Logger.getLogger(getClass());
	Hashtable<String, String> hash = new Hashtable<String, String>();

	String mainQuery = null;
	String neighborQuery = null;
	String altQuery = null;
	String altMainQuery = null;
	String altQuery2 = null;
	boolean subject = true;
	
	boolean instance = false;
	// core class for neighbor hoods etc.
	/**
	 * Constructor for AbstractRelationPopup.
	 * @param name 				Constructs a new JMenu with this string as its text.
	 * @param ps 				Playsheet that popup appears on.
	 * @param pickedVertex 		Picked vertex.
	 * @param mainQuery			Main query.
	 * @param neighborQuery 	Neighbor query - used for normal neighborhoods.
	 * @param altQuery 			Alternate query - used when the picked vertex is hosed.
	 * @param altMainQuery 		Alternate main query - used when the selected vertex is hosed in the main query.
	 * @param altQuery2 		Second alternate query - used when the selected vertex and next vertex are hosed.
	 */
	public AbstractRelationPopup(String name, IPlaySheet ps, SEMOSSVertex [] pickedVertex, String mainQuery, String neighborQuery, String altQuery, String altMainQuery, String altQuery2)
	{
		super(name);
		// need to get this to read from popup menu
		this.ps = ps;
		this.pickedVertex = pickedVertex;
		this.mainQuery = mainQuery;
		this.neighborQuery = neighborQuery; // used when everything is fine
		this.altQuery = altQuery; // used when the selected vertex is hosed
		this.altQuery2 = altQuery2; // used when the selected vertex and the next vertex is hosed
		this.altMainQuery = altMainQuery;// when the selected vertex is hosed for main query
		addMouseListener(this);
	}
	
	/**
	 * Executes the query and adds relationships depending on the specified URI.
	 // TODO: Parameter not used
	 * @param suffix String
	 */
	public void addRelations(String suffix)
	{

		// execute the query
		// add all the relationships
		// the relationship needs to have the subject - selected vertex
		// need to add the relationship to the relationship URI
		// and the predicate selected
		// the listener should then trigger the graph play sheet possibly
		// and for each relationship add the listener
		String typeQuery =  DIHelper.getInstance().getProperty(this.neighborQuery); 

		String ignoreURI = DIHelper.getInstance().getProperty(Constants.IGNORE_URI);
		for(int pi = 0;pi < pickedVertex.length;pi++)
		{
			SEMOSSVertex thisVert = pickedVertex[pi];
			String uri = thisVert.getURI();

			String query2 = DIHelper.getInstance().getProperty(this.mainQuery);

			if(uri.contains("/"))
			{
				Pattern pattern = Pattern.compile("\\s");
				Matcher matcher = pattern.matcher(uri);
				boolean found = matcher.find();
				if(found)
				{
					uri = "\"" + Utility.getInstanceName(uri) +"\"";
					query2 = DIHelper.getInstance().getProperty(this.altMainQuery);
					logger.info("Changing the type query to " + typeQuery);
				}
				else
					uri = "<" + uri + ">";
			}
			else if(!uri.contains("\""))
				uri = "\"" + uri + "\"";
			
			
			hash.put(Constants.URI, uri);
			String filledQuery = Utility.fillParam(query2, hash);
			
			// get the repository and execute the query
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
			sjw.setQuery(filledQuery);
			sjw.executeQuery();
			
			logger.debug("Executed Query");
			
			String [] vars = sjw.getVariables();
			while(sjw.hasNext())
			{
				SesameJenaSelectStatement stmt = sjw.next();

				String predName = stmt.getRawVar(vars[0]) + "";
								
				boolean dirty = false;
				
				String predClassName; 
				String uriName = uri;
				
				if(!instance)
					predClassName = Utility.getQualifiedClassName(predName);
				else
					predClassName = predName;

				predClassName = predClassName.trim();
				
				if(predClassName.contains("/")) 
				{
					// if it has spaces
					// then strip it and use alternate query otherwise do this
					Pattern pattern = Pattern.compile("\\s");
					Matcher matcher = pattern.matcher(predClassName);
					boolean found = matcher.find();
					if(found)
					{
						// handle the predicate first
						if(instance)
							predClassName = "\"" + Utility.getInstanceName(predClassName) +"\"";
						else // ok this is fucked up
							dirty = true;
						
						typeQuery = DIHelper.getInstance().getProperty(this.altQuery);
						
						// now see if the main one also needs to be switched around
						String vertURI = thisVert.getURI();
						vertURI = vertURI.trim();
						matcher = pattern.matcher(vertURI);
						found = matcher.find();
						if(vertURI.contains("/"))
						{
							if(found)
							{
								uriName = Utility.getInstanceName(vertURI);
								typeQuery = DIHelper.getInstance().getProperty(this.altQuery2);
							}								
						}
						//logger.info("Changing the type query to " + typeQuery);
						if(!instance)
							dirty = true;
					}
					else
						predClassName = "<" + predClassName + ">";
				}
				else if(!predName.contains("\""))
					predClassName = "\"" + predClassName + "\"";
				
				hash.put(Constants.URI, uriName);
				hash.put(Constants.PREDICATE, predClassName);
				//logger.debug("Predicate is " + predName + "<<>> "+ predClassName);
				
				String nFillQuery = Utility.fillParam(typeQuery, hash);
				//logger.debug("Filler Query is " + nFillQuery);
				// compose the query based on this class name
				// should we get type or not ?
				// that is the question
				logger.info("Trying predicate class name for " + predClassName + " instance is " + instance);
				if(!dirty && predClassName.length() > 0 && 
						!Utility.checkPatternInString(ignoreURI, predClassName) && 
						!hash.containsKey(predClassName) && 
						(!instance && !predClassName.contains("\"")) || instance)
				{
					logger.info("Filler Query is  " + nFillQuery);
					NeighborMenuItem nItem = new NeighborMenuItem(predClassName, nFillQuery, engine);
					nItem.addActionListener(NeighborMenuListener.getInstance());
					add(nItem);
					hash.put(predClassName, predClassName);
				}
				// for each of these relationship add a relationitem
			
			}			
		}
		repaint();
	}

	/**
	 * Invoked when the mouse button has been clicked (pressed and released) on a component.
	 * Adds relationships given a specific suffix.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseClicked(MouseEvent arg0) {
		logger.info("Mouse Entered and Clicked");
		//addRelations("?subject", "?object");
		addRelations("");
		addRelations("_2");
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
