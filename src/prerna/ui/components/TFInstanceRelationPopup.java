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
import java.util.Vector;

import javax.swing.JLabel;
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
 * This class is used to create a popup menu for the TF instance relation.
 */
public class TFInstanceRelationPopup extends JMenu implements MouseListener{
	
	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	Logger logger = Logger.getLogger(getClass());

	String mainQuery = null;
	String neighborQuery = null;
	
	boolean instance = false;
	boolean populated = false;
	// core class for neighbor hoods etc.
	/**
	 * Constructor for TFInstanceRelationPopup.
	 * @param name String
	 * @param ps IPlaySheet
	 * @param pickedVertex DBCMVertex[]
	 */
	public TFInstanceRelationPopup(String name, IPlaySheet ps, SEMOSSVertex [] pickedVertex)
	{
		super(name);
		// need to get this to read from popup menu
		this.ps = ps;
		this.pickedVertex = pickedVertex;
		this.mainQuery = Constants.NEIGHBORHOOD_TYPE_QUERY;
		this.neighborQuery = Constants.TRAVERSE_FREELY_QUERY;
		addMouseListener(this);
	}
	
	/**
	 * Executes query and adds appropriate relations.
	 * @param prefix	Prefix used to create the type query.
	 */
	public void addRelations(String prefix)
	{

		// get the selected repository
				JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
				Object [] repos = (Object [])list.getSelectedValues();
				// I am only going to get one repository
				// hopefully they have selected one :)
				String repo = repos[0] +"";
				IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo);
				// execute the query
				// add all the relationships
				// the relationship needs to have the subject - selected vertex
				// need to add the relationship to the relationship URI
				// and the predicate selected
				// the listener should then trigger the graph play sheet possibly
				// and for each relationship add the listener
				String typeQuery =  DIHelper.getInstance().getProperty(this.neighborQuery + prefix);
				Hashtable<String, String> hash = new Hashtable<String, String>();
				String ignoreURI = DIHelper.getInstance().getProperty(Constants.IGNORE_URI);
				int count = 0;
				Vector typeV = new Vector();
				for(int pi = 0;pi < pickedVertex.length;pi++)
				{		
					SEMOSSVertex thisVert = pickedVertex[pi];
					String uri = thisVert.getURI();

					String query2 = DIHelper.getInstance().getProperty(this.mainQuery+ prefix);
					String typeName = Utility.getConceptType(engine, thisVert.uri);
					if(typeV.contains(typeName))
					{
						continue;
					}
					else
					{
					typeV.addElement(typeName);
					}
					
					if(prefix.equals(""))
						hash.put("SUBJECT_TYPE", typeName);
					else
						hash.put("OBJECT_TYPE", typeName);
					
					// get the filter values
					String fileName = "";
					for(int vertIndex = 0;vertIndex < pickedVertex.length;vertIndex++)
					{
						if (pickedVertex[vertIndex].getProperty(Constants.VERTEX_TYPE).equals(thisVert.getProperty(Constants.VERTEX_TYPE)))
						fileName = fileName + "(<" + pickedVertex[vertIndex].getURI() + ">)";
					}
					
					//put in param hash and fill  
					hash.put("FILTER_VALUES", fileName);
					String filledQuery = Utility.fillParam(query2, hash);
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
						// only one variable
						String objClassName = stmt.getRawVar(vars[0])+"";
						//logger.debug("Predicate is " + predName + "<<>> "+ predClassName);

						//logger.debug("Filler Query is " + nFillQuery);
						// compose the query based on this class name
						// should we get type or not ?
						// that is the question
						logger.debug("Trying predicate class name for " + objClassName );
						if(objClassName.length() > 0 && !Utility.checkPatternInString(ignoreURI, objClassName)
								&& !objClassName.equals("http://semoss.org/ontologies/Concept")
								&& !objClassName.equals("http://www.w3.org/2000/01/rdf-schema#Resource"))
						{
							String instance = Utility.getInstanceName(objClassName);
							//add the to: and from: labels
							if(count == 0){
								if(this.getItemCount()>0)
									addSeparator();
								if(prefix.equals(""))
									addLabel("To:");
								else
									addLabel("From:");
							}
							count++;

							logger.debug("Adding Relation " + objClassName);

							if(prefix.equals(""))
								hash.put("OBJECT_TYPE", objClassName);
							else
								hash.put("SUBJECT_TYPE", objClassName);
							
							String nFillQuery = Utility.fillParam(typeQuery, hash);

							NeighborMenuItem nItem = new NeighborMenuItem(instance, nFillQuery, engine);
							nItem.addActionListener(NeighborMenuListener.getInstance());
							add(nItem);
							//hash.put(objClassName, predClassName);
						}
						// for each of these relationship add a relationitem

					}			
				}
				populated = true;
				repaint();
	}

	/**
	 * Invoked when the mouse button has been clicked (pressed and released) on a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseClicked(MouseEvent arg0) {
		//addRelations("");
		//addRelations("_2");
	}

	/**
	 * Invoked when the mouse enters a component.
	 * @param arg0 MouseEvent
	 */
	@Override
	public void mouseEntered(MouseEvent arg0) {
		if(!populated)
		{
			addRelations("");
			addRelations("_2");
		}
	//	addRelations();
		
	}
	
	/**
	 * Adds label.
	 * @param label 	Label, in string form.
	 */
	public void addLabel(String label){
		add(new JLabel(label));
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
