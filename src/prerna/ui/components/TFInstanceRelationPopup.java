/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.ui.components;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JMenu;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabase;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.wrappers.WrapperManager;
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
	static final Logger logger = LogManager.getLogger(TFInstanceRelationPopup.class.getName());

	String mainQuery, mainQueryJENA, neighborQuery, neighborQueryJENA;
	
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
		this.mainQueryJENA = Constants.NEIGHBORHOOD_TYPE_QUERY_JENA;
		this.neighborQuery = Constants.TRAVERSE_FREELY_QUERY;
		this.neighborQueryJENA = Constants.TRAVERSE_FREELY_QUERY_JENA;
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
				IDatabase engine = (IDatabase)DIHelper.getInstance().getLocalProp(repo);
				
				boolean isRDF = (engine.getDatabaseType() == IDatabase.DATABASE_TYPE.SESAME || engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA || 
						engine.getDatabaseType() == IDatabase.DATABASE_TYPE.SEMOSS_SESAME_REMOTE);

				// execute the query
				// add all the relationships
				// the relationship needs to have the subject - selected vertex
				// need to add the relationship to the relationship URI
				// and the predicate selected
				// the listener should then trigger the graph play sheet possibly
				// and for each relationship add the listener
				String typeQuery = "";
				if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA) {
					typeQuery = DIHelper.getInstance().getProperty(this.neighborQueryJENA + prefix);
				} else {
					typeQuery =  DIHelper.getInstance().getProperty(this.neighborQuery + prefix);
				}
				Map<String, List<Object>> hash = new Hashtable<String, List<Object>>();
				String ignoreURI = engine.getProperty(Constants.IGNORE_URI);
				//if(ignoreURI == null)
					
				int count = 0;
				Vector typeV = new Vector();
				for(int pi = 0;pi < pickedVertex.length;pi++)
				{		
					SEMOSSVertex thisVert = pickedVertex[pi];
					String uri = thisVert.getURI();
					String displayUri = thisVert.getURI();
//					uri = Utility.getTransformedNodeName(engine, uri, false);
					
					String query2 = "";
					if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA) {
						query2 = DIHelper.getInstance().getProperty(this.mainQueryJENA + prefix);
					} else {
						query2 = DIHelper.getInstance().getProperty(this.mainQuery + prefix);
					}
					String typeName = null;
					if(isRDF)
						typeName = Utility.getConceptType(engine, uri);
					else if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.RDBMS)
						typeName = Utility.getQualifiedClassName(uri);

					if(typeV.contains(typeName))
					{
						continue;
					}
					else
					{
					typeV.addElement(typeName);
					}
					
					if(prefix.equals("")) {
						List<Object> typeList = new ArrayList<Object>();
						typeList.add(typeName);
						hash.put("SUBJECT_TYPE", typeList);
					} else {
						List<Object> typeList = new ArrayList<Object>();
						typeList.add(typeName);
						hash.put("OBJECT_TYPE", typeList);
					}
					// get the filter values
					String fileName = "";
					
					// Replacing this call with the simple engines get neighborhood call
					Vector <String> neighbors = engine.getNeighbors(typeName, 0);
					if(isRDF)
					{
						
						if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA) {
							for(int vertIndex = 0;vertIndex < pickedVertex.length;vertIndex++)
							{
								if (pickedVertex[vertIndex].getProperty(Constants.VERTEX_TYPE).toString().equals(thisVert.getProperty(Constants.VERTEX_TYPE).toString())){
//									fileName = fileName + "<" + Utility.getTransformedNodeName(engine, pickedVertex[vertIndex].getURI(), false) + ">";
									fileName = fileName + "<" + pickedVertex[vertIndex].getURI() + ">";

								}
							}
						} else {
							for(int vertIndex = 0;vertIndex < pickedVertex.length;vertIndex++)
							{
								if (pickedVertex[vertIndex].getProperty(Constants.VERTEX_TYPE).toString().equals(thisVert.getProperty(Constants.VERTEX_TYPE).toString())){
//									fileName = fileName + "(<" + Utility.getTransformedNodeName(engine, pickedVertex[vertIndex].getURI(), false) + ">)";
									fileName = fileName + "<" + pickedVertex[vertIndex].getURI() + ">";
								}
							}
						}
	
						//put in param hash and fill  
						List<Object> fileNameList = new ArrayList<Object>();
						fileNameList.add(fileName);
						hash.put("FILTER_VALUES", fileNameList);
						String filledQuery = Utility.fillParam(query2, hash);
						logger.debug("Found the engine for repository   " + repo);
	
						ISelectWrapper sjw = WrapperManager.getInstance().getSWrapper(engine, filledQuery);
	
						// run the query
						/*SesameJenaSelectWrapper sjw = new SesameJenaSelectWrapper();
						sjw.setEngine(engine);
						sjw.setEngineType(engine.getDatabaseType());
						sjw.setQuery(filledQuery);
						sjw.executeQuery();
						*/
						logger.debug("Executed Query");
	
						String [] vars = sjw.getVariables();
						while(sjw.hasNext())
						{
							ISelectStatement stmt = sjw.next();
							// only one variable
							String objClassName = stmt.getRawVar(vars[0])+"";
							String pred = "";
							if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA) {
								pred = stmt.getRawVar(vars[1])+"";
							}
							//logger.debug("Predicate is " + predName + "<<>> "+ predClassName);
	
							//logger.debug("Filler Query is " + nFillQuery);
							// compose the query based on this class name
							// should we get type or not ?
							// that is the question
							logger.debug("Trying predicate class name for " + objClassName );
							if(objClassName.length() > 0 && !Utility.checkPatternInString(ignoreURI, objClassName)
									&& !objClassName.equals("http://semoss.org/ontologies/Concept")
									&& !objClassName.equals("http://www.w3.org/2000/01/rdf-schema#Resource") 
									&& !objClassName.equals("http://www.w3.org/2000/01/rdf-schema#Class")
									&& !pred.equals("http://semoss.org/ontologies/Relation")
									&& (pred.equals("") || pred.startsWith("http://semoss.org")))
							{
//								String instance = Utility.getInstanceName(Utility.getTransformedNodeName(engine, objClassName+"", true));
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
	
								if(prefix.equals("")) {
									List<Object> typeList = new ArrayList<Object>();
									typeList.add(objClassName);
									hash.put("OBJECT_TYPE", typeList);
								} else {
									List<Object> typeList = new ArrayList<Object>();
									typeList.add(objClassName);
									hash.put("SUBJECT_TYPE", typeList);
								}
								if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA) {
									List<Object> predList = new ArrayList<Object>();
									predList.add(pred);
									hash.put("PREDICATE", predList);
								}
								
								String nFillQuery = Utility.fillParam(typeQuery, hash);
								System.err.println(nFillQuery);
								NeighborMenuItem nItem = new NeighborMenuItem(instance, nFillQuery, engine);
								if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA) {
									nItem = new NeighborMenuItem("->" + Utility.getInstanceName(pred) + "->" + instance, nFillQuery, engine);
								}
								nItem.addActionListener(NeighborMenuListener.getInstance());
								add(nItem);
								//hash.put(objClassName, predClassName);
							}
							// for each of these relationship add a relationitem
	
						}	
					}
					else if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.RDBMS)
					{
					// block to uncomment later
						
						Vector <String> fromInstances = new Vector<String>();
						for(int vertIndex = 0;vertIndex < pickedVertex.length;vertIndex++)
							fromInstances.add(pickedVertex[vertIndex].getURI());
						
						for(int nIndex = 0;nIndex < neighbors.size();nIndex++)
						{
							String neighbor = neighbors.get(nIndex);	
//							String instance = Utility.getInstanceName(Utility.getTransformedNodeName(engine, neighbor, true));
							String instance = Utility.getInstanceName(neighbor);

							// get the query here from engine and embed it
							// It uses three things
							// current class name, new type name, instance
							// for now I am going to cast this based on RDBMS
							
							RDBMSNativeEngine rEngine = null;
							String traverseQuery = "";
							if(engine instanceof RDBMSNativeEngine)
							{
								rEngine = (RDBMSNativeEngine)engine;
								traverseQuery = rEngine.traverseOutputQuery(typeName, neighbor, fromInstances);
								System.out.println("Query is...  " + traverseQuery);
								
							}
							
							NeighborMenuItem nItem = new NeighborMenuItem(instance, traverseQuery, engine);
							if(engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA) {
								nItem = new NeighborMenuItem(instance, traverseQuery, engine);
							}
							nItem.addActionListener(NeighborMenuListener.getInstance());
							add(nItem);
							
						}			
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
			
			JList list = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
			Object [] repos = (Object [])list.getSelectedValues();
			// I am only going to get one repository
			// hopefully they have selected one :)
			String repo = repos[0] +"";
			IDatabase engine = (IDatabase)DIHelper.getInstance().getLocalProp(repo);
			
			boolean isRDF = (engine.getDatabaseType() == IDatabase.DATABASE_TYPE.SESAME || engine.getDatabaseType() == IDatabase.DATABASE_TYPE.JENA || 
					engine.getDatabaseType() == IDatabase.DATABASE_TYPE.SEMOSS_SESAME_REMOTE);
			if(isRDF)
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
