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
package prerna.ui.components.specific.tap;

import java.beans.PropertyVetoException;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.RepositoryException;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.DIHelper;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;

/**
 //TODO: Remove class? Never called
 */
public class SOATransitionAllSheet extends GraphPlaySheet{
	
	Logger logger = Logger.getLogger(getClass());
	
	/**
	 * Constructor for SOATransitionAllSheet.
	 */
	public SOATransitionAllSheet()
	{
		super();
		
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore =new Hashtable<String, SEMOSSEdge>();

		rdfMap = DIHelper.getInstance().getRdfMap();
		createBaseURIs();
		logger.info("Graph PlaySheet " + query);
		//addInitialPanel();
		//addPanel();
	}
	
	/**
	 * Create the specified layout for the perspective and create the visualization viewer.
	 */
	public void finalizeVisualizer(){

		createLayout();
		createVisualizer();
		addPanel();
		// addpane
		// addpane
		legendPanel.drawLegend();
		
		showAll();
		// activate the frame if it is the second time
		try {
			this.setSelected(false);
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		
		printConnectedNodes();
		printSpanningTree();
		//progressBarUpdate("100%...SOA Transition Complete", 100);
	}

	
	/**
	 * Extends the SOA view.
	 */
	public void extendSOAView()
	{
		updateProgressBar("50%...Extending Services", 50);
		showAll();
		try
		{
			overlay = false;
			extend = true;

			curModel = null;
			if(query.toUpperCase().contains("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();
			sjw.setEngine(engine);
			sjw.setQuery(query);
			sjw.execute();

			extend = true;
			createForest();
			
			// create the specified layout
			logger.debug("Adding the new model " + curModel);
			modelStore.addElement(curModel);
			rcStore.addElement(curRC);
			logger.debug("Extend : Total Models added = " + modelStore.size());
			//queryPropAll();
		}catch(Exception ex)
		{
			ex.printStackTrace();
			logger.fatal(ex);
		}
	}
	/**
	 * Generates the data and recreates the legend for a refined SOA view.
	 */
	public void refineSOAView() {
		// need to include the relation vs. prop logic

		getForest();
		genBaseGraph();
		genAllData();
		legendPanel.data = filterData;
		// create the specified layout
		// addpane
		// addpane
		legendPanel.drawLegend();
		genAllData();
		logger.info("Refining Forest Complete >>>>>");
	}

	/**
	 * Repaint the display with the SOA view.
	 */
	public void recreateSOAView() {
		// need to include the relation vs. prop logic
		try {
			// get the graph query result and paint it
			// need to get all the vertex transformers here

			// create initial panel
			// addInitialPanel();
			// execute the query now
			
			//writeStatus(" Starting create view");
			updateProgressBar("40%...Recreate Graph", 40);
			
			//curModel = null;
			addInitialPanel();
			addToMainPane(pane);
			showAll();
			
			getForest();
			if(!loadedOWLS.containsKey(engine.getEngineName()) && engine instanceof AbstractEngine)
			{
				if(this.baseRelEngine == null) this.baseRelEngine = ((AbstractEngine)engine).getBaseDataEngine();
				else RDFEngineHelper.addAllData(((AbstractEngine)engine).getBaseDataEngine(), this.baseRelEngine.getRC());
				RDFEngineHelper.addAllData(baseRelEngine, rc);
				loadedOWLS.put(engine.getEngineName(), engine.getEngineName());
			}
			genBaseConcepts();
			genBaseGraph();
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getStackTrace());
		}
		
	}
	
	// this process is to remove an existing  forest
	// will come back to this later
	// this is going to be tricky
	// I need to remove the nodes not only from the forest.. but I need to realign the whole vertex filter data as well
	// I bet I will come back to this after the engines
	
	/**
	 * Removes the SOA interface from the view.
	 */
	public void removeSOAView()
	{
		showAll();
		// this will extend it
		// i.e. Checks to see if the node is available
		// if the node is not already there then this predicate wont be added

		logger.info("Removing Data from Forest >>>>>");
		
		if(query.toUpperCase().contains("CONSTRUCT"))
			sjw = new SesameJenaConstructWrapper();
		else
			sjw = new SesameJenaSelectCheater();

		updateProgressBar("70%...Removing Interfaces", 70);
		sjw.setEngine(engine);
		sjw.setQuery(query);
		sjw.execute();

		edgeVector = new Vector();

		Model curModel = ModelFactory.createDefaultModel();
		
		while (sjw.hasNext()) {
			String delQuery = "DELETE {?s ?p ?o} WHERE{";
			// logger.info("Iterating ...");
			SesameJenaConstructStatement st = sjw.next();
			org.openrdf.model.Resource subject = new URIImpl(st.getSubject());
			org.openrdf.model.URI predicate = new URIImpl(st.getPredicate());
			
			// figure out if this is an object later
			Object obj = st.getObject();
			delQuery=delQuery+"BIND(<"+subject+">AS ?s) BIND(<"+predicate+"> AS ?p) BIND(<"+obj+"> AS ?o){?s ?p ?o}}";
	
			logger.debug(subject+","+predicate+","+obj);
			Update up;
			try {
				up = rc.prepareUpdate(QueryLanguage.SPARQL, delQuery);
				rc.setAutoCommit(false);
				up.execute();
			} catch (RepositoryException e) {
				e.printStackTrace();
			} catch (MalformedQueryException e) {
				e.printStackTrace();
			} catch (UpdateExecutionException e) {
				e.printStackTrace();
			}
		}
		
		refineSOAView();
		finalizeVisualizer();
		logger.debug("Removing Forest Complete >>>>>> ");
	}
}
