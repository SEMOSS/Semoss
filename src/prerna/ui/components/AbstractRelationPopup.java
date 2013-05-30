package prerna.ui.components;

import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.JList;
import javax.swing.JMenu;

import org.apache.log4j.Logger;

import prerna.om.DBCMVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.NeighborMenuListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractRelationPopup extends JMenu implements MouseListener{
	
	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	// sets the picked node list
	DBCMVertex [] pickedVertex = null;
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
	public AbstractRelationPopup(String name, IPlaySheet ps, DBCMVertex [] pickedVertex, String mainQuery, String neighborQuery, String altQuery, String altMainQuery, String altQuery2)
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
			DBCMVertex thisVert = pickedVertex[pi];
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

	@Override
	public void mouseClicked(MouseEvent arg0) {
		System.out.println("Mouse Entered and Clicked");
		//addRelations("?subject", "?object");
		addRelations("");
		addRelations("_2");
	}

	@Override
	public void mouseEntered(MouseEvent arg0) {
		// TODO Auto-generated method stub
		System.out.println("Mouse Entered and Clicked");
	//	addRelations();
		
	}

	@Override
	public void mouseExited(MouseEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mousePressed(MouseEvent arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void mouseReleased(MouseEvent arg0) {
		// TODO Auto-generated method stub
		
	}	
}
