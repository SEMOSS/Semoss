package prerna.ui.components.playsheets;

import java.beans.PropertyVetoException;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.apache.log4j.Logger;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.engine.api.IEngine;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.main.listener.impl.PlaySheetListener;
import prerna.ui.main.listener.impl.SimpleGraphListener;
import prerna.util.Utility;
import edu.uci.ics.jung.algorithms.layout.FRLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateForest;
import edu.uci.ics.jung.visualization.VisualizationViewer;

public class SQLGraphPlaysheet extends AbstractRDFPlaySheet {
	
	Logger logger = Logger.getLogger(getClass());
	
	// visualizationviewer
	public VisualizationViewer <SEMOSSVertex, SEMOSSEdge> view = null;
	public DelegateForest forest = new DelegateForest();
	SimpleGraphListener viewListener = new SimpleGraphListener();
	
	public void setBaseProperties()
	{
		setResizable(true);
		setClosable(true);
		setMaximizable(true);
		setIconifiable(true);		
		Layout myLayout = new FRLayout<SEMOSSVertex, SEMOSSEdge>(forest);
		view = new VisualizationViewer<SEMOSSVertex, SEMOSSEdge>(myLayout);
		view.setGraphMouse(viewListener);
		this.addInternalFrameListener(new PlaySheetListener());
		
	}
	
	@Override
	public void createView()
	{
		setBaseProperties();
		try {
			// set window call
	
			JPanel hello = new JPanel();
			hello.add(new JLabel("Hello World"));
			this.add(new JButton("Hello"));
			this.add(hello);
			
			SEMOSSVertex vert1 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/Title/Hello");
			SEMOSSVertex vert2 = new SEMOSSVertex("http://semoss.org/ontologies/Concept/Studio/Fox");
			SEMOSSEdge edge = new SEMOSSEdge(vert1, vert2, "http://semoss.org/ontologies/relation/Hello:Fox");
			
			forest.addEdge(edge, vert1, vert2);
			
			
			
			this.add(view);
			//view.setBackground(Color.blue);
			
			pane.add(this);
			//pane.add(view);
			
			this.pack();
			this.setVisible(true);
			this.setSelected(false);
			this.setSelected(true);
			logger.debug("Added the main pane");
			
			// how the process would work
			/*
			 *  a. A Particular entity is clicked
			 *  b. Based on the entity, we determine what is the class of this entity that is being clicked and find all the neighbors
			 *  c. There has to be something in the OWL which says this entity class is mapped to this table (we dont have this now)
			 *  d. See if properties are required on the call
			 *  e. If so find all the properties for this entity
			 *  f. Pass this method to the engine
			 *  g. Engine now composes the query and fetches everything we need
			 *  
			 *  
			 *  
			 *  
			 */
			String className = Utility.getClassName(vert1.uri); // a.

			Vector <String> neighbors = engine.getNeighbors("http://semoss.org/ontologies/Concept/Title", 0); //b.
			// get the properties mapped for each of this
			
			
			
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void refineView() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void createData() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
		
	}
	@Override
	public Hashtable<String, String> getDataTableAlign() {
		// TODO Auto-generated method stub
		return null;
	}
	
	// simple add
	public void addMore(IEngine engine, String query)
	{
		// job here is really simple
		// get the wrapper manager
		// take the information
		// paint it
		IConstructWrapper cheater = WrapperManager.getInstance().getChWrapper(engine, query);
		while(cheater.hasNext())
		{
			IConstructStatement stmt = cheater.next();
			
			SEMOSSVertex vert1 = new SEMOSSVertex(stmt.getSubject());
			SEMOSSVertex vert2 = new SEMOSSVertex(stmt.getObject()+"");
			SEMOSSEdge edge = new SEMOSSEdge(vert1, vert2, stmt.getPredicate());
			
			forest.addEdge(edge, vert1, vert2);
			
		}		
	}


}
