package prerna.ui.components;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JInternalFrame;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JTabbedPane;
import javax.swing.Painter;
import javax.swing.UIDefaults;
import javax.swing.UIManager;

import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.KruskalMinimumSpanningTree;
import org.jgrapht.graph.SimpleGraph;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.om.DBCMEdge;
import prerna.om.DBCMVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.InMemoryJenaEngine;
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.GraphNodeListener;
import prerna.ui.main.listener.impl.PlaySheetColorShapeListener;
import prerna.ui.main.listener.impl.PlaySheetControlListener;
import prerna.ui.main.listener.impl.PlaySheetListener;
import prerna.ui.main.listener.impl.PlaySheetOWLListener;
import prerna.ui.transformer.ArrowDrawPaintTransformer;
import prerna.ui.transformer.ArrowFillPaintTransformer;
import prerna.ui.transformer.EdgeArrowStrokeTransformer;
import prerna.ui.transformer.EdgeLabelTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.EdgeTooltipTransformer;
import prerna.ui.transformer.SearchEdgeStrokeTransformer;
import prerna.ui.transformer.SearchVertexLabelFontTransformer;
import prerna.ui.transformer.SearchVertexPaintTransformer;
import prerna.ui.transformer.VertexIconTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexLabelTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.ui.transformer.VertexShapeTransformer;
import prerna.ui.transformer.VertexStrokeTransformer;
import prerna.ui.transformer.VertexTooltipTransformer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.reasoner.ReasonerRegistry;

import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateForest;
import edu.uci.ics.jung.visualization.GraphZoomScrollPane;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.renderers.BasicRenderer;
import edu.uci.ics.jung.visualization.renderers.Renderer;

public class GraphPlaySheet extends JInternalFrame implements IPlaySheet{

	/*
	 * this will have references to the following a. Internal Frame that needs to be displayed b. The panel of
	 * parameters c. The composed SPARQL Query d. Perspective selected e. The question selected by the user f. Filter
	 * criterias including slider values
	 */
	protected String query = null;
	protected ParamPanel panel = null;
	protected SesameJenaConstructWrapper sjw = null;
	public DelegateForest forest = null;
	Hashtable <String, String> loadedOWLS = new Hashtable<String, String>();
	VisualizationViewer <DBCMVertex, DBCMEdge> view = null;
	String layoutName = Constants.FR;
	Layout layout2Use = null;
	JTabbedPane rightView = null;
	protected String title = null;
	protected IEngine engine = null;
	public LegendPanel2 legendPanel = null;
	public JPanel cheaterPanel = new JPanel();
	public JProgressBar jBar = new JProgressBar();
	public JTabbedPane jTab = new JTabbedPane();
	public Vector edgeVector = new Vector();
	public JInternalFrame dataLatencyPopUp = null;
	
	//So that it doesn't get reset on extend and overlay etc. it must be stored
	VertexLabelFontTransformer vlft;

	
	protected SimpleGraph <DBCMVertex, DBCMEdge> graph = new SimpleGraph<DBCMVertex, DBCMEdge>(DBCMEdge.class);
	
	public VertexFilterData filterData = new VertexFilterData();
	ControlData controlData = new ControlData();
	PropertySpecData predData = new PropertySpecData();
	VertexColorShapeData colorShapeData = new VertexColorShapeData();
	protected String questionNum = null;
	public JComponent pane = null;
	
	protected boolean append = false;
	protected boolean extend = false;
	
	// references to main vertstore
	protected Hashtable<String, DBCMVertex> vertStore = null;
	// references to the main edgeStore
	protected Hashtable<String, DBCMEdge> edgeStore = null;
	// checks to see if we already added a particular set of vertices
	// if so tracks it as the same edge
	
	// base filter hash
	protected Hashtable<String, String> baseFilterHash = new Hashtable<String, String>();
	
	protected Properties rdfMap = null;
	protected String RELATION_URI = null;
	protected String PROP_URI = null;
	protected Logger logger = Logger.getLogger(getClass());
	public SearchPanel searchPanel = new SearchPanel();
	public JPanel graphPanel = new JPanel();
	public RepositoryConnection rc = null;
	protected RepositoryConnection curRC = null;
	protected Model jenaModel = null;
	protected Model curModel = null;
	
	protected Vector <Model> modelStore = new Vector<Model>();
	protected Vector <RepositoryConnection> rcStore = new Vector<RepositoryConnection>();


	public GraphPlaySheet()
	{
		super("_", true, true, true, true);
		
		this.setPreferredSize(new Dimension(800,600));
		vertStore = new Hashtable<String, DBCMVertex>();
		edgeStore =new Hashtable<String, DBCMEdge>();

		rdfMap = DIHelper.getInstance().getRdfMap();
		createBaseURIs();
		logger.info("Graph PlaySheet " + query);
		//addInitialPanel();
		//addPanel();
	}
	
	public GraphPlaySheet(String title, String query, ParamPanel panel,
			IEngine engine, String questionNum) {
		super(title, true, true, true, true);
		this.panel = panel;
		this.query = query;
		this.engine = engine;
		this.title = title;
		this.questionNum = questionNum;

	}
	
	public void setTitle(String title)
	{
		super.setTitle(title);
		this.title = title;
	}
	
	public void setRDFEngine(IEngine engine)
	{
		logger.info("Set the engine " );
		this.engine = engine;
	}
	
	public void setQuestionID(String questionNum)
	{
		this.questionNum = questionNum;
	}
	
	public String getQuestionID()
	{
		return this.questionNum;
	}

	public void setAppend(boolean append) {
		logger.debug("Append set to " + append);
		//writeStatus("Append set to  : " + append);
		this.append = append;
	}
	
	public void setExtend(boolean extend)
	{
		logger.debug("Extend set to " + extend);
		//writeStatus("Extend set to  : " + extend);
		this.extend = extend;
	}
	public void copyView() {
		try {
			
			//writeStatus(" Copying Graph");
			getForest();
			
			curModel = null;
			addInitialPanel();
			addToMainPane(pane);
			showAll();
			sjw.execute();
			logger.debug("Executed the select");
			
			createForest();
			createLayout();
			createVisualizer();
			//writeStatus(" Completed creating forest ");
			//writeStatus("Completed Layout ");
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			// add the panel
			addPanel();
			// addpane
			//addToMainPane();
			//showAll();
			// activate the frame if it is the second time
			this.setSelected(false);
			this.setSelected(true);
			
			printConnectedNodes();
			printSpanningTree();

		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getStackTrace());
		}
	}

	public void createView() {
		try {
			// get the graph query result and paint it
			// need to get all the vertex transformers here

			// create initial panel
			// addInitialPanel();
			// execute the query now
			setAppend(false);
			setExtend(false);
			
			//writeStatus(" Starting create view");
			getForest();
			
			curModel = null;
			addInitialPanel();
			addToMainPane(pane);
			showAll();

			if(query.toUpperCase().contains("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();

			//writeStatus(" Created the queries ");

			sjw.setEngine(engine);
			progressBarUpdate("10%...Querying RDF Repository", 10);
			sjw.setQuery(query);
			progressBarUpdate("30%...Querying RDF Repository", 30);
			sjw.execute();
			progressBarUpdate("60%...Processing RDF Statements	", 60);
			
			//writeStatus("\n" + "Painting Graph" + "\n");

			logger.debug("Executed the select");
			
			createForest();
			progressBarUpdate("80%...Creating Visualization", 80);
			createLayout();
			createVisualizer();
			//writeStatus(" Completed creating forest ");
			//writeStatus("Completed Layout ");
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			// add the panel
			addPanel();
			// addpane
			//addToMainPane();
			//showAll();
			// activate the frame if it is the second time
			this.setSelected(false);
			this.setSelected(true);
			
			printConnectedNodes();
			printSpanningTree();
			
			progressBarUpdate("100%...Graph Generation Complete", 100);

		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getStackTrace());
		}
	}
	
	@Override
	public void redoView() {
		try {
			// get the graph query result and paint it
			// need to get all the vertex transformers here

			// create initial panel
			// addInitialPanel();
			// execute the query now
			setAppend(false);
			setExtend(false);
			
			getForest();
			
			if(query.toUpperCase().contains("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();


			sjw.setEngine(engine);
			progressBarUpdate("10%...Querying RDF Repository", 10);
			sjw.setQuery(query);
			progressBarUpdate("30%...Querying RDF Repository", 30);
			sjw.execute();
			progressBarUpdate("60%...Processing RDF Statements	", 60);

			
			if(!extend)
				refineView();
			else
				extendForest();
			progressBarUpdate("80%...Creating Visualization", 80);
			createLayout();
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			// add the panel
			addPanel();

			this.setSelected(false);
			this.setSelected(true);
			
			printConnectedNodes();
			printSpanningTree();
			progressBarUpdate("100%...Graph Recreation Complete", 100);

		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getStackTrace());
		}
		
	}

	
	public void overlayView()
	{
		try {
			extend = false;

			append = false;

			curModel = null;
			if(query.toUpperCase().contains("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();
			
			sjw.setEngine(engine);
			progressBarUpdate("10%...Querying RDF Repository", 10);
			sjw.setQuery(query);
			progressBarUpdate("30%...Querying RDF Repository", 30);
			sjw.execute();
			progressBarUpdate("60%...Processing RDF Statements	", 60);
			
			createForest();
			progressBarUpdate("80%...Creating Visualization", 80);
			
			// create the specified layout
			logger.debug("Adding the new model " + curModel);
			modelStore.addElement(curModel);
			rcStore.addElement(curRC);
			logger.debug("Overlay - Total Models added = " + modelStore.size());
			createLayout();
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			createVisualizer();
			// add the panel
			addPanel();
			// addpane
			showAll();
			// activate the frame if it is the second time
			this.setSelected(false);
			this.setSelected(true);

			printConnectedNodes();
			printSpanningTree();
			progressBarUpdate("100%...Graph Overlay Complete", 100);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.fatal(e);
			e.printStackTrace();
		}
		
	}
	
	public void extendView()
	{
		try
		{
			append = false;
			extend = true;

			curModel = null;
			if(query.toUpperCase().contains("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();
			sjw.setEngine(engine);
			progressBarUpdate("10%...Querying RDF Repository", 10);
			sjw.setQuery(query);
			progressBarUpdate("30%...Querying RDF Repository", 30);
			sjw.execute();

			extend = true;
			createForest();
			progressBarUpdate("80%...Creating Visualization", 80);
			
			// create the specified layout
			logger.debug("Adding the new model " + curModel);
			modelStore.addElement(curModel);
			rcStore.addElement(curRC);
			logger.debug("Extend : Total Models added = " + modelStore.size());

			createLayout();
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			createVisualizer();
			// add the panel
			addPanel();
			//legendPanel.drawLegend();
			// addpane
			// show it
			this.setSelected(false);
			this.setSelected(true);
			printConnectedNodes();
			printSpanningTree();
			progressBarUpdate("100%...Graph Extension Complete", 100);
		}catch(Exception ex)
		{
			ex.printStackTrace();
			logger.fatal(ex);
		}
	}
	public void extendProp(String query)
	{
		try
		{
			append = false;
			extend = true;

			curModel = null;
			if(query.toUpperCase().contains("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();
			sjw.setEngine(engine);
			//progressBarUpdate("10%...Querying RDF Repository", 10);
			sjw.setQuery(query);
			//progressBarUpdate("30%...Querying RDF Repository", 30);
			sjw.execute();

			extend = true;
			createForest();
			
			// create the specified layout
			logger.debug("Adding the new model " + curModel);
			//modelStore.addElement(curModel);
			logger.debug("Extend : Total Models added = " + modelStore.size());


			this.setSelected(false);
			this.setSelected(true);
			printConnectedNodes();
			printSpanningTree();
			//progressBarUpdate("100%...Adding Properties Complete", 100);
		}catch(Exception ex)
		{
			ex.printStackTrace();
			logger.fatal(ex);
		}
	}
	public void removeView2()
	{
		// this will extend it
		// i.e. Checks to see if the node is available
		// if the node is not already there then this predicate wont be added

		logger.info("Removing Data from Forest >>>>>");
		
		if(query.toUpperCase().contains("CONSTRUCT"))
			sjw = new SesameJenaConstructWrapper();
		else
			sjw = new SesameJenaSelectCheater();

		sjw.setEngine(engine);
		progressBarUpdate("10%...Querying RDF Repository", 10);
		sjw.setQuery(query);
		progressBarUpdate("30%...Querying RDF Repository", 30);
		sjw.execute();
		progressBarUpdate("60%...Processing RDF Statements	", 60);

		edgeVector = new Vector();

		int count = 0;
		
		Model curModel = ModelFactory.createDefaultModel();
		while (sjw.hasNext()) {
			// System.out.println("Iterating ...");
			SesameJenaConstructStatement st = sjw.next();

			Resource subject = jenaModel.createResource(st.getSubject());
			Property prop = jenaModel.createProperty(st.getPredicate());
			com.hp.hpl.jena.rdf.model.Statement jenaSt = curModel.createStatement(
					subject, prop, st.getObject()+"");
			curModel.add(jenaSt);
			Hashtable<String, String> filteredNodes = filterData.filterNodes;
			// write out what you are removing
			// and find a place to show it
			if(jenaModel.contains(jenaSt))
			{
				String predicate = st.getPredicate();
				if (Utility.checkPatternInString(RELATION_URI, predicate)
						&& !rdfMap.contains(predicate)
						&& !Utility.checkPatternInString(PROP_URI, predicate)) // need to change this to starts with
																				// relation
				{
					logger.debug(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
					String subURI = st.getSubject();
					String objURI = st.getObject()+"";
					if (!filteredNodes.containsKey(subURI)
							&& !filteredNodes.containsKey(objURI) && !filterData.edgeFilterNodes.containsKey(st.getPredicate())) {

							
								String strArray[] = (st.getSubject()+"").split("/");
								int size = strArray.length;
								String edgeName = strArray[size-1];
								strArray = (st.getObject()+"").split("/");
								size = strArray.length;
								edgeName = edgeName +"->"+strArray[size-1];
								
								edgeVector.addElement(edgeName);
								//System.out.println(edgeName);

					}
				//System.out.println(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
				logger.debug("Removing the statement ");
				//writeStatus("Removing " + st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
				jenaModel.remove(jenaSt);
				//edgeVector.addElement(jenaSt.getSubject()+"-"+jenaSt.getPredicate()+"-"+jenaSt.getObject());
				count++;
			}

				
			
		}
		}
		progressBarUpdate("80%...Creating Visualization", 80);
		//writeStatus("Total Statements Dropped " + count);
		// test call

		refineView();
		logger.debug("Removing Forest Complete >>>>>> ");
		progressBarUpdate("100%...Graph Remove Complete", 100);
	}
	
	public void removeView()
	{
		// this will extend it
		// i.e. Checks to see if the node is available
		// if the node is not already there then this predicate wont be added

		logger.info("Removing Data from Forest >>>>>");
		if(query.toUpperCase().contains("CONSTRUCT"))
			sjw = new SesameJenaConstructWrapper();
		else
			sjw = new SesameJenaSelectCheater();

		sjw.setEngine(engine);
		progressBarUpdate("10%...Querying RDF Repository", 10);
		sjw.setQuery(query);
		progressBarUpdate("30%...Querying RDF Repository", 30);
		sjw.execute();
		progressBarUpdate("60%...Processing RDF Statements	", 60);

		edgeVector = new Vector();

		int count = 0;
		
		Model curModel = ModelFactory.createDefaultModel();
		String delQuery = "DELETE DATA {";
		while (sjw.hasNext()) {
			// System.out.println("Iterating ...");
			SesameJenaConstructStatement st = sjw.next();
			org.openrdf.model.Resource subject = new URIImpl(st.getSubject());
			org.openrdf.model.URI predicate = new URIImpl(st.getPredicate());
			
			// figure out if this is an object later
			Object obj = st.getObject();
			delQuery=delQuery+"{<"+subject+"><"+predicate+">";
	
			if((obj instanceof com.hp.hpl.jena.rdf.model.Literal)||(obj instanceof com.bigdata.rdf.model.BigdataValueImpl))
			{
	
				delQuery=delQuery+obj;
			}
			else 
			{
				delQuery=delQuery+"<"+obj+">";
			}
			
			delQuery = delQuery+"}";
			count++;
			System.out.println(count);
		}
			
		
		Update up;
		try {
			up = rc.prepareUpdate(QueryLanguage.SPARQL, delQuery);
			rc.setAutoCommit(false);
			up.execute();
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UpdateExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//sc.addStatement(vf.createURI("<http://health.mil/ontologies/dbcm/Concept/Service/tom2>"),vf.createURI("<http://health.mil/ontologies/dbcm/Relation/Exposes>"),vf.createURI("<http://health.mil/ontologies/dbcm/Concept/BusinessLogicUnit/tom1>"));
		System.out.println("\nSPARQL: " + query);
		//tq.setIncludeInferred(true /* includeInferred */);
		//tq.evaluate();

		genBaseGraph("","","");
		progressBarUpdate("80%...Creating Visualization", 80);
		//writeStatus("Total Statements Dropped " + count);
		// test call

		refineView();
		logger.debug("Removing Forest Complete >>>>>> ");
		progressBarUpdate("100%...Graph Remove Complete", 100);
	}
	/*
	public void undoView2()
	{
		// get the latest and undo it
		// Need to find a way to keep the base relationships
		if(modelStore.size() > 0)
		{
			Model lastModel = modelStore.elementAt(modelStore.size() - 1);
			// remove it from jena model
			logger.info("Number of new statements " + lastModel.size());
			logger.info("Number of statements in the old model " + jenaModel.size());
			StmtIterator stmti = lastModel.listStatements();
			while(stmti.hasNext())
			{
				Statement sct = stmti.next();
				// only undo if it is not base relationships
				// at a later date need to see if the base relationships completely remove everything from this engine and if so remove that too
				// but for now :)
				if(!baseFilterHash.containsKey(sct.getSubject()) && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
					jenaModel.remove(sct);
			}
			progressBarUpdate("20%...Querying RDF Repository", 20);
			
			modelStore.remove(modelStore.size() - 1);
			
			refineView();
		}
		progressBarUpdate("100%...Graph Undo Complete", 100);
	}
*/
	public void undoView()
	{
		// get the latest and undo it
		// Need to find a way to keep the base relationships
		try {
			if(modelStore.size() > 0)
			{
				RepositoryConnection lastRC = rcStore.elementAt(rcStore.size() - 1);
				// remove it from jena model
				logger.info("Number of new statements " + lastRC.size());
				logger.info("Number of statements in the old model " + rc.size());
				RepositoryResult<org.openrdf.model.Statement> stmti = lastRC.getStatements(null, null, null, true);
				while(stmti.hasNext())
				{
					org.openrdf.model.Statement sct = stmti.next();
					// only undo if it is not base relationships
					// at a later date need to see if the base relationships completely remove everything from this engine and if so remove that too
					// but for now :)
					if(!baseFilterHash.containsKey(sct.getSubject()) && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
						rc.remove(sct);
				}
				progressBarUpdate("20%...Querying RDF Repository", 20);
				
				rcStore.remove(modelStore.size() - 1);
				
				refineView();
			}
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		progressBarUpdate("100%...Graph Undo Complete", 100);
	}

	
	public void refineView()
	{
		try {
			getForest();
			genBaseGraph("","","");
			//progressBarUpdate("80%...Creating Visualization", 80);
			
			String containsRelation = "<http://health.mil/ontologies/dbcm/Relation/Contains>";

			if(containsRelation != null)
			{
			
				// now that this is done, we can query for concepts
				String propertyQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
				  "{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
				  //"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://health.mil/ontologies/dbcm/Concept>;}" +
				  		"{?Subject ?Predicate ?Object}}";					

				genPropertiesLocal(propertyQuery);
			}

			genAllData();
			logger.info("Refining Forest Complete >>>>>");
			
			// create the specified layout
			createLayout();
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			createVisualizer();
			// add the panel
			addPanel();
			// addpane
			// addpane
			legendPanel.drawLegend();
			//showAll();
			//progressBarUpdate("100%...Graph Refine Complete", 100);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	public void refreshView(){
		createVisualizer();
		// add the panel
		addPanel();
		try {
			this.setSelected(false);
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//showAll();
	}

	
	public void recreateView() {
		// need to include the relation vs. prop logic
		try {
			// get the graph query result and paint it
			// need to get all the vertex transformers here

			// create initial panel
			// addInitialPanel();
			// execute the query now
			
			//writeStatus(" Starting create view");
			getForest();
			
			//curModel = null;
			addInitialPanel();
			addToMainPane(pane);
			showAll();

			modelStore.addElement(curModel);
			rcStore.addElement(curRC);
			createLayout();
			createVisualizer();

			addPanel();
			// activate the frame if it is the second time
			this.setSelected(false);
			this.setSelected(true);
			
			printConnectedNodes();
			printSpanningTree();
			progressBarUpdate("100%...Graph Creation Complete", 100);
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getStackTrace());
		}
		
	}
	
	public void setJDesktopPane(JComponent pane)
	{
		this.pane = pane;
	}
	
	public void addInitialPanel()
	{
		
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[]{728, 0};
		gridBagLayout.rowHeights = new int[]{557, 70, 0};
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		getContentPane().setLayout(gridBagLayout);
		
	
		// create the listener and add the frame
		// JInternalFrame frame = new JInternalFrame(title, true, true, true, true);
		// frame.setPreferredSize(new Dimension(400,600));
		// if there is a view remove it
		// get
		cheaterPanel.setPreferredSize(new Dimension(800, 70));
		GridBagLayout gbl_cheaterPanel = new GridBagLayout();
		gbl_cheaterPanel.columnWidths = new int[]{0, 0};
		gbl_cheaterPanel.rowHeights = new int[]{60, 0};
		gbl_cheaterPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_cheaterPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		cheaterPanel.setLayout(gbl_cheaterPanel);

		legendPanel = new LegendPanel2();
		legendPanel.setPreferredSize(new Dimension(800,50));
		GridBagConstraints gbc_legendPanel = new GridBagConstraints();
		gbc_legendPanel.fill = GridBagConstraints.BOTH;
		gbc_legendPanel.gridx = 0;
		gbc_legendPanel.gridy = 0;
		cheaterPanel.add(legendPanel, gbc_legendPanel);
		
		jBar.setStringPainted(true);
		jBar.setString("0%...Preprocessing");
		jBar.setValue(0);
		UIDefaults nimbusOverrides = new UIDefaults();
		UIDefaults defaults = UIManager.getLookAndFeelDefaults();
		defaults.put("nimbusOrange",defaults.get("nimbusInfoBlue"));
		Painter blue = (Painter) defaults.get("Button[Default+Focused+Pressed].backgroundPainter");
        nimbusOverrides.put("ProgressBar[Enabled].foregroundPainter",blue);
        jBar.putClientProperty("Nimbus.Overrides", nimbusOverrides);
        jBar.putClientProperty("Nimbus.Overrides.InheritDefaults", false);
        
       // SwingUtilities.updateComponentTreeUI(jBar);
		GridBagConstraints gbc_jBar = new GridBagConstraints();
		gbc_jBar.anchor = GridBagConstraints.NORTH;
		gbc_jBar.fill = GridBagConstraints.HORIZONTAL;
		gbc_jBar.gridx = 0;
		gbc_jBar.gridy = 1;
		cheaterPanel.add(jBar, gbc_jBar);
		GridBagConstraints gbc_cheaterPanel = new GridBagConstraints();
		gbc_cheaterPanel.anchor = GridBagConstraints.NORTH;
		gbc_cheaterPanel.fill = GridBagConstraints.HORIZONTAL;
		gbc_cheaterPanel.gridx = 0;
		gbc_cheaterPanel.gridy = 1;
		this.getContentPane().add(cheaterPanel, gbc_cheaterPanel);
		
		GridBagConstraints gbc_jTab = new GridBagConstraints();
		gbc_jTab.anchor = GridBagConstraints.NORTH;
		gbc_jTab.fill = GridBagConstraints.BOTH;
		gbc_jTab.gridx = 0;
		gbc_jTab.gridy = 0;
		this.getContentPane().add(jTab, gbc_jTab);
		jTab.insertTab("Graph", null, graphPanel, null, 0);

	}
	
	protected void addPanel() {
		
		// add the model to search panel
		//searchPanel.indexStatements(jenaModel);
		
		// create the listener and add the frame
		// JInternalFrame frame = new JInternalFrame(title, true, true, true, true);
		// frame.setPreferredSize(new Dimension(400,600));
		// if there is a view remove it
		// get
		/*
		Component[] comps = getContentPane().getComponents();
		for (int compIndex = 0; compIndex < comps.length; compIndex++) {
			logger.debug("Component is " + comps[compIndex]);
			if (comps[compIndex] instanceof JScrollPane) {
				logger.debug("Removing the component");
				remove(comps[compIndex]);
			}
		}
		*/
		
		graphPanel.removeAll();
		//graphPanel.setLayout(new BorderLayout());
		GraphZoomScrollPane gzPane = new GraphZoomScrollPane(view);
		gzPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		gzPane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
		//view.setPreferredSize(new Dimension(800,600));
		//view.setMaximumSize(new Dimension(800,600));
		//JScrollPane myPane = new JScrollPane(view);
		//myPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		//myPane.getHorizontalScrollBar().setUI(new NewScrollBarUI());
		//myPane.setViewportView(view);
		//myPane.setAutoscrolls(true);
		
		//graphPanel.add(searchPanel, BorderLayout.NORTH);
		//graphPanel.add(gzPane, BorderLayout.CENTER);
		//jTab.insertTab("Graph", null, graphPanel, null, 0);
		GridBagLayout gbl_graphPanel = new GridBagLayout();
		gbl_graphPanel.columnWidths = new int[]{0, 0};
		gbl_graphPanel.rowHeights = new int[]{0, 0, 0};
		gbl_graphPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_graphPanel.rowWeights = new double[]{0.0, 1.0, Double.MIN_VALUE};
		graphPanel.setLayout(gbl_graphPanel);
		
		GridBagConstraints gbc_search = new GridBagConstraints();
		gbc_search.insets = new Insets(0, 0, 5, 0);
		gbc_search.fill = GridBagConstraints.BOTH;
		gbc_search.gridx = 0;
		gbc_search.gridy = 0;
		graphPanel.add(searchPanel, gbc_search);
		
		GridBagConstraints gbc_panel_2 = new GridBagConstraints();
		gbc_panel_2.fill = GridBagConstraints.BOTH;
		gbc_panel_2.gridx = 0;
		gbc_panel_2.gridy = 1;
		graphPanel.add(gzPane, gbc_panel_2);
		//jTab.setSelectedIndex(0);
		
		//testPanel.add(jTab, BorderLayout.NORTH);
		//testPanel.add(new JButton("Hula Hoop"), BorderLayout.PAGE_END);
		//testPanel.add(new JTextArea("Hula Hoop"), BorderLayout.PAGE_END);
		
	
		//testPanel.add(myPane);
		//this.getContentPane().add(testPanel, BorderLayout.PAGE_START);
		

		//this.setAutoscrolls(true);
		this.addInternalFrameListener(PlaySheetListener.getInstance());
		this.addInternalFrameListener(PlaySheetControlListener.getInstance());
		this.addInternalFrameListener(PlaySheetOWLListener.getInstance());
		this.addInternalFrameListener(PlaySheetColorShapeListener.getInstance());
		this.addComponentListener(
				new ComponentAdapter(){
					public void componentResized(ComponentEvent e){
						System.out.println(((JInternalFrame)e.getComponent()).isMaximum());
						GraphPlaySheet gps = (GraphPlaySheet) e.getSource();
						Dimension s = new Dimension(gps.view.getWidth(), gps.view.getHeight()-gps.cheaterPanel.HEIGHT);
						layout2Use.setSize(view.getSize());
						System.out.println("Size: " + gps.view.getSize());
						
					}
				});


		legendPanel.data = filterData;
		legendPanel.drawLegend();
		logger.info("Add Panel Complete >>>>>");
	}

	protected void addToMainPane(JComponent pane) {

		pane.add((Component)this);

		logger.info("Adding Main Panel Complete");
	}

	public void showAll() {
		this.pack();
		this.setVisible(true);
		//JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
	//			Constants.MAIN_FRAME);
		//frame2.repaint();

	}

	protected void createVisualizer() {
		this.layout2Use.setSize(new Dimension(this.getContentPane().getWidth()-15, this.getContentPane().getHeight()-cheaterPanel.getHeight()-(int)searchPanel.getPreferredSize().getHeight()-50));
		view = new VisualizationViewer(this.layout2Use);
		view.setPreferredSize(this.layout2Use.getSize());
		view.setBounds(10000000, 10000000, 10000000, 100000000);
		


		Renderer r = new BasicRenderer();
		
		view.setRenderer(r);
		//view.getRenderer().setVertexRenderer(new MyRenderer());

		GraphNodeListener gl = new GraphNodeListener();
		view.setGraphMouse(new GraphNodeListener());
		// DefaultModalGraphMouse gm = new DefaultModalGraphMouse();
		gl.setMode(ModalGraphMouse.Mode.PICKING);
		view.setGraphMouse(gl);
		VertexLabelTransformer vlt = new VertexLabelTransformer(controlData);
		VertexPaintTransformer vpt = new VertexPaintTransformer();
		VertexShapeTransformer vsht = new VertexShapeTransformer();
		VertexTooltipTransformer vtt = new VertexTooltipTransformer(controlData);
		EdgeLabelTransformer elt = new EdgeLabelTransformer(controlData);
		EdgeTooltipTransformer ett = new EdgeTooltipTransformer(controlData);
		EdgeStrokeTransformer est = new EdgeStrokeTransformer();
		VertexStrokeTransformer vst = new VertexStrokeTransformer();
		ArrowDrawPaintTransformer adpt = new ArrowDrawPaintTransformer();
		EdgeArrowStrokeTransformer east = new EdgeArrowStrokeTransformer();
		ArrowFillPaintTransformer aft = new ArrowFillPaintTransformer();
		//keep the stored one if possible
		if(vlft==null)
			vlft = new VertexLabelFontTransformer();
		VertexIconTransformer vit = new VertexIconTransformer();
		
		//view.getRenderContext().getGraphicsContext().setStroke(s);

		Color color = view.getBackground();
		view.setBackground(Color.WHITE);
		color = view.getBackground();
		
		//view.setGraphMouse(mc);
		view.getRenderContext().setVertexLabelTransformer(
							vlt);
		view.getRenderContext().setEdgeLabelTransformer(
				elt);
		view.getRenderContext().setVertexStrokeTransformer(vst);
		view.getRenderContext().setVertexShapeTransformer(vsht);
		view.getRenderContext().setVertexFillPaintTransformer(
				vpt);
		view.getRenderContext().setEdgeStrokeTransformer(est);
		view.getRenderContext().setArrowDrawPaintTransformer(adpt);
		view.getRenderContext().setEdgeArrowStrokeTransformer(east);
		view.getRenderContext().setArrowFillPaintTransformer(aft);
		view.getRenderContext().setVertexFontTransformer(vlft);
		//view.getRenderContext().set;
		// view.getRenderContext().setVertexIconTransformer(new DBCMVertexIconTransformer());
		view.setVertexToolTipTransformer(vtt);
		view.setEdgeToolTipTransformer(ett);
		//view.getRenderContext().setVertexIconTransformer(vit);
		controlData.setViewer(view);
		searchPanel.setViewer(view);
		logger.info("Completed Visualization >>>> ");
	}

	public boolean createLayout() {
		int fail = 0;
		// creates the layout
		// Constructor cons = Class.forName(layoutName).getConstructor(this.forest.class);
		// layout2Use = (Layout)cons.newInstance(forest);
		logger.info("Create layout >>>>>> ");
		Class layoutClass = (Class)DIHelper.getInstance().getLocalProp(layoutName);
		//layoutClass.getConstructors()
		Constructor constructor=null;
		try{
			constructor = layoutClass.getConstructor(edu.uci.ics.jung.graph.Forest.class);
			layout2Use  = (Layout)constructor.newInstance(forest);
		}catch(Exception e){
			fail++;
			logger.info(e);
		}
		try{
			constructor = layoutClass.getConstructor(edu.uci.ics.jung.graph.Graph.class);
			layout2Use  = (Layout)constructor.newInstance(forest);
		}catch(Exception e){
			fail++;
			logger.info(e);
		}
		//= (Layout) new FRLayout((forest));
		logger.info("Create layout Complete >>>>>> ");
		if(fail==2) return false;
		else return true;
	}
	
	public String getLayoutName(){
		return layoutName;
	}
	
	
	protected void createForest() throws Exception
	{
		// need to take the base information from the base query and insert it into the jena model
		// this is based on EXTERNAL ontology
		// then take the ontology and insert it into the jena model
		// (may be eventually we can run this through a reasoner too)
		// Now insert our base model into the same ontology
		// Now query the model for 
		// Relations - Paint the basic graph
		// Now find a way to get all the predicate properties from them
		// Hopefully the property is done using subproperty of
		// predicates - Pick all the predicates but for the properties
		// paint them
		// properties
		// and then paint it appropriately
		logger.debug("creating the in memory jena model");
		
		String subjects = "";
		String subjects2 = "";
		String predicates = "";
		String predicates2 = "";
		String objects = "";
		String objects2 = "";
		
		while(sjw.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model
			SesameJenaConstructStatement st = sjw.next();
			Object obj = st.getObject();
			logger.debug(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
			//predData.addPredicate2(st.getPredicate());
			if(!subjects.contains(st.getSubject()))
			{
				subjects = subjects + "(<" + st.getSubject() + ">)";
				subjects2 = subjects2 + "<" + st.getSubject() + ">";
			}
			if(!predicates.contains(st.getPredicate()))
			{
				predicates = predicates + "(<" + st.getPredicate() + ">) ";
				predicates2 = predicates2 + "<" + st.getPredicate() + "> ";
			}
			// need to find a way to do this for jena too
			if(obj instanceof URI)
			{			
				if(!objects.contains(obj+""))
				{
					objects = objects + "(<" + obj + ">)";
					objects2 = objects2 + "<" + obj + ">";				
				}
			}
			//addToJenaModel(st);
			addToJenaModel2(st, false);
			//addToJenaModel3(st);

		}
		
		logger.debug("Subjects >>> " + subjects);
		logger.debug("Predicatss >>>> " + predicates);
		
		// now add the base relationships to the metamodel
		// this links the hierarchy that tool needs to the metamodel being queried
		// eventually this could be a SPIN
		// need to get the engine name and jam it - Done Baby
		if(!loadedOWLS.containsKey(engine.getEngineName()))
		{
			createBaseRelations(DIHelper.getInstance().getEngineProp(), "BaseData");
			loadedOWLS.put(engine.getEngineName(), engine.getEngineName());
		}

		// load the concept linkages
		// the concept linkages are a combination of the base relationships and what is on the file
		if (subjects.equals("") && predicates.equals("") && objects.equals(""))
		{
			
		}
		else
		{
			subjects2 = loadConceptHierarchy(subjects, objects);
			predicates2 = loadRelationHierarchy(predicates);
		}
		// then query the database for concepts
		// get all the concepts
		//subjects2 = findAllConcepts(subjects2);
		// then query this model for everything that is beginning with that
		
		
		logger.warn("Creating the base Graph");

		genBaseGraph(subjects2, predicates2, subjects2);
		
		//find the contains property
		// Need to do the properties piece shortly
		// done
		// get it a single shot
		// find the name of the properties relation
		String containsRelation = findContainsRelation();
		logger.warn("Creating the properties");
		if(containsRelation != null)
		{
			// load local property hierarchy
			loadPropertyHierarchy(predicates, containsRelation);
		
			// now that this is done, we can query for concepts
			String propertyQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
			  "{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
			  //"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://health.mil/ontologies/dbcm/Concept>;}" +
			  		"{?Subject ?Predicate ?Object}}";
					

			genPropertiesRemote(propertyQuery + "BINDINGS ?Subject { " + subjects + " " + predicates + " " + objects+ " } ");
			genPropertiesLocal(propertyQuery);
			//genProperties(propertyQuery + predicates + " } ");
		}
		logger.warn("Done with everything");
		genAllData();
		// first execute all the predicate selectors
		// Backdoor entry

		Thread thread = new Thread(){
			public void run()
			{
				printAllRelationship();				
			}
		};
		thread.start();
		
		logger.info("Creating Forest Complete >>>>>> ");										
	}
	
	private String findAllConcepts(String subjects)
	{
		String subjectString = "";
		
		String conceptSelector = "SELECT DISTINCT ?Subject WHERE " +
		"{" +
		"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://health.mil/ontologies/dbcm/Concept>;}" +
		"}";
		
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		
		IEngine jenaEngine = new InMemoryJenaEngine();
		((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		
		// = new SesameJenaSelectCheater();
		sjsw.setEngine(jenaEngine);
		sjsw.setQuery(conceptSelector);//conceptHierarchyForSubject);
		sjsw.executeQuery();
		sjsw.getVariables();
		
		// eventually - I will not need the count
		int count = 0;
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement st = sjsw.next();
			subjectString = subjectString + "<" + st.getRawVar("Subject") + ">";
		}
		
		
		return subjectString.length()==0?null:subjectString;

	}
	
    private void printAllRelationship()
    {
          String conceptHierarchyForSubject = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
          "{" +
          "{?Subject ?Predicate ?Object}" + 
          "}";
          logger.debug(conceptHierarchyForSubject);
          
          IEngine jenaEngine = new InMemorySesameEngine();
          ((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);
          
          SesameJenaConstructWrapper sjsc;
          
          if(query.toUpperCase().contains("CONSTRUCT"))
                sjsc = new SesameJenaConstructWrapper();
          else
                sjsc = new SesameJenaSelectCheater();

          // = new SesameJenaSelectCheater();
          sjsc.setEngine(jenaEngine);
          /*sjsc.setQuery(query);//conceptHierarchyForSubject);
          sjsc.execute();
          
          logger.warn("Printing All Relationship for based query based on JenaModel");
          logger.warn(">>>>");
          while(sjsc.hasNext())
          {
                // read the subject predicate object
                // add it to the in memory jena model
                // get the properties
                // add it to the in memory jena model
                SesameJenaConstructStatement st = sjsc.next();
                logger.warn(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
                //addToJenaModel(st);
          }
          */
          logger.warn("<<<<");
          String end = "";
          
                while(!end.equalsIgnoreCase("end"))
                {
                      try {
                      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                      System.out.println("Enter Query");
                      String query2 = reader.readLine();                    
                      end = query2;
                      System.out.println("Query is " + query2);
                      if(query2.toUpperCase().contains("CONSTRUCT"))
                            sjsc = new SesameJenaConstructWrapper();
                      else
                            sjsc = new SesameJenaSelectCheater();

                      // = new SesameJenaSelectCheater();
                      sjsc.setEngine(jenaEngine);
                      sjsc.setQuery(query);//conceptHierarchyForSubject);
                      sjsc.setQuery(query2);
                      sjsc.execute();
                      while(sjsc.hasNext())
                      {
                            // read the subject predicate object
                            // add it to the in memory jena model
                            // get the properties
                            // add it to the in memory jena model
                            SesameJenaConstructStatement st = sjsc.next();
                            logger.warn(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
                            //addToJenaModel(st);
                      }
                      } catch (Exception e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                      }
                }

          
    }

	
	private String findContainsRelation()
	{
		String query2 = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {{?Subject ?Predicate ?Object}" +
				       "{?Object <http://www.w3.org/2000/01/rdf-schema#subPropertyOf>  <http://health.mil/ontologies/dbcm/Relation/Contains>}}";

		String containsString = null;
		
		SesameJenaConstructWrapper sjsc = new SesameJenaConstructWrapper();
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		
		if(query2.toUpperCase().contains("CONSTRUCT"))
			sjsc = new SesameJenaConstructWrapper();
		else
			sjsc = new SesameJenaSelectCheater();

		// = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);
		sjsc.setQuery(query);//conceptHierarchyForSubject);
		sjsc.setQuery(query2);
		sjsc.execute();
		
		// eventually - I will not need the count
		int count = 0;
		while(sjsc.hasNext() && count < 1)
		{
			SesameJenaConstructStatement st = sjsc.next();
			containsString = "<" + st.getSubject() + ">";
			count++;
		}
		
		
		return containsString;
	}
	
	
	private String loadConceptHierarchy(String subjects, String objects)
	{
		// Load all the various types of these subjects
		// possibilities are these subjects have one parent or one of many parents
		// Possibly one of these parents is linked with the ontology linker file
		// which is why I load these
		String retSubjects = "";
		String conceptHierarchyForSubject = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
		"{" +
		"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?Object}" +
		"{?Subject ?Predicate ?Object}" + 
		"} BINDINGS ?Subject { " + subjects + objects + " } " +
		"";
		logger.warn(conceptHierarchyForSubject);
		
		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(engine);
		sjsc.setQuery(conceptHierarchyForSubject);
		sjsc.execute();
		
		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model
			SesameJenaConstructStatement st = sjsc.next();
			logger.debug(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
			predData.addConcept(st.getObject()+"", st.getSubject()+"");
			if(!baseFilterHash.containsKey(st.getSubject()))
				retSubjects = retSubjects + "<" + st.getSubject() + ">";

			// I have to have some logic which will add the type name 
			// basically the object is the main type
			// subject is the subtype
			//addToJenaModel(st);
			addToJenaModel2(st, false);
		}
		
		return retSubjects;

	}
	
	private String loadRelationHierarchy(String predicates)
	{
		// same concept as the subject, but only for relations
		String relationHierarchy = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
		"{" +
		"{?Subject ?Predicate ?Object}" + 
		"{?Subject <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> ?Object}" +
		"} BINDINGS ?Subject { " + predicates + " } " +
		"";// relation hierarchy
		
		String retRelations = "";

		
		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(engine);
		sjsc.setQuery(relationHierarchy);
		sjsc.execute();
		
		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model
			
			SesameJenaConstructStatement st = sjsc.next();
			logger.debug(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
			predData.addPredicate2(st.getObject()+"", st.getSubject());
			if(!baseFilterHash.containsKey(st.getSubject()))
				retRelations = retRelations + "<" + st.getSubject() + ">";
			// I have to have some logic which will add the type name 
			// basically the object is the main type
			// subject is the subtype
			//addToJenaModel(st);
			addToJenaModel2(st, false);
		}
		return retRelations;
	}

	private void loadPropertyHierarchy(String predicates, String containsRelation)
	{
		// same concept as the subject, but only for relations
		String relationHierarchy = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE " +
		"{" +
		"{?Subject ?Predicate ?Object}" + 
		"{?Subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " + containsRelation + " }" +
		"} BINDINGS ?Subject { " + predicates + " } " +
		"";// relation hierarchy
		
		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(engine);
		sjsc.setQuery(relationHierarchy);
		sjsc.execute();
		
		while(sjsc.hasNext())
		{
			// read the subject predicate object
			// add it to the in memory jena model
			// get the properties
			// add it to the in memory jena model
			
			SesameJenaConstructStatement st = sjsc.next();
			logger.debug(st.getSubject() + "<<>>" + st.getPredicate() + "<<>>" + st.getObject());
			predData.addPredicate2(st.getObject()+"", st.getSubject());
			// I have to have some logic which will add the type name 
			// basically the object is the main type
			// subject is the subtype
			
			//addToJenaModel(st);
			addToJenaModel2(st, false);
		}
	}

	
	// executes the first SPARQL query and generates the graphs
	public void genBaseGraph(String subjects, String predicates, String objects)
	{
		// create all the relationships now
		String predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
									  //"VALUES ?Subject {"  + subjects + "}"+
									  //"VALUES ?Object {"  + subjects + "}"+
									  //"VALUES ?Object {"  + objects + "}" +
									  //"VALUES ?Predicate {"  + predicates + "}" +
									  "{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://health.mil/ontologies/dbcm/Relation>;}" +
									  "{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://health.mil/ontologies/dbcm/Concept>;}" +
									  "{?Object " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://health.mil/ontologies/dbcm/Concept>;}" +
									  "{?Subject ?Predicate ?Object}" +
									  "}";
		
		System.err.println(" Predicate query " + predicateSelectQuery);
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);

		Hashtable<String, String> filteredNodes = filterData.filterNodes;
		logger.warn("Filtered Nodes " + filteredNodes);
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
		
		logger.debug(predicateSelectQuery);
		
		try {
			sjsc.setQuery(predicateSelectQuery);
			sjsc.execute();
			logger.warn("Execute complte");

			int count = 0;
			while(sjsc.hasNext())
			{
				//logger.warn("Iterating " + count);
				count++;
				SesameJenaConstructStatement sct = sjsc.next();
				if(!baseFilterHash.containsKey(sct.getSubject()) && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
				{
						// get the subject, predicate and object
						// look for the appropriate vertices etc and paint it
						predData.addConceptAvailable(sct.getSubject());
						DBCMVertex vert1 = vertStore.get(sct.getSubject()+"");
						if(vert1 == null)
						{
							vert1 = new DBCMVertex(sct.getSubject());
							vertStore.put(sct.getSubject()+"", vert1);
							genControlData(vert1);
						}
						DBCMVertex vert2 = vertStore.get(sct.getObject()+"");
						if(vert2 == null)
						{
							vert2 = new DBCMVertex(sct.getObject()+"");
							vertStore.put(sct.getObject()+"", vert2);
							genControlData(vert2);
						}
						// create the edge now
						DBCMEdge edge = edgeStore.get(sct.getPredicate()+"");
						if(edge == null)
						{
							edge = new DBCMEdge(vert1, vert2, sct.getPredicate());
							edgeStore.put(sct.getPredicate()+"", edge);
						}
						filterData.addVertex(vert1);
						filterData.addVertex(vert2);
						filterData.addEdge(edge);
						//logger.warn("Found Edge " + edge.getURI() + "<<>>" + vert1.getURI() + "<<>>" + vert2.getURI());

						
						// add the edge now if the edge does not exist
						// need to handle the duplicate issue again
						try
						{
							if ((filteredNodes == null) || (filteredNodes != null && !filteredNodes.containsKey(sct.getSubject()+"")
									&& !filteredNodes.containsKey(sct.getObject() +"") && !filterData.edgeFilterNodes.containsKey(sct.getPredicate() + ""))) 						{	
								predData.addPredicate(sct.getPredicate());
								// try to see if the predicate here is a property
								// if so then add it as a property
							this.forest.addEdge(edge, vertStore.get(sct.getSubject()+""),
								vertStore.get(sct.getObject()+""));
							genControlData(edge);
							// to be removed later
							// I dont know if we even use this
							// need to ask Bill and Tom
							graph.addVertex(vertStore.get(sct.getSubject()));
							graph.addVertex(vertStore.get(sct.getObject()+""));
							
							graph.addEdge(vertStore.get(sct.getSubject()),
									vertStore.get(sct.getObject()+""), edge);
							}
						}catch (Exception ex)
						{
							logger.warn("Missing Edge " + edge.getURI() + "<<>>" + vert1.getURI() + "<<>>" + vert2.getURI());
							// ok.. I am going to ignore for now that this is a duplicate edge
						}
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//}		
	}
	
	public void genPropertiesRemote(String query2)
	{
		// this needs to be redone to accomodate external ontology
		// this needs to be a method specific to TAP
		// because there is no way for me to generically understand properties
		// may be they can specify a property class or set of classes
		// if so I can load that with the bindings ?
		// not sure *Thinking*
		// Since I have loaded all of the relationship hierarchy
		// 
		
		// Ok.. I see what I need to get done
		// I need to find all the relations that are subclass to contains
		// but in order for me to do this.. i need to get EVERY relation and then jam it
		// or alternately, I can just get this from the base graph
		// now based on those things that I just aggregated i.e. the predicates
		// I need to query the core database for those relations for any subject
		// interestingly there is no dual bind i.e. a bind for relationship and a bind for subjects
		// so I have to find a way to do this using a nested select
		// for now - I am going to assume all of the properties is done using only ONE and ONLY ONE URI patter
		// I will select it using SPARQL first
		// and then use that relationship to get all the properties
		// Later when BigData will support VALUES on SPARQL 1.1, I can do multi-bind and mystery solved
		// oh yes, for now if you have more than one then I will assume only the first one


		/*IEngine jenaEngine = new InMemoryJenaEngine();
		((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);
		*/
		logger.debug("Executing Query " + query2);
		
		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(engine);
		sjsc.setQuery(query2);
		sjsc.execute();
		
		while(sjsc.hasNext())
		{
			SesameJenaConstructStatement sct = sjsc.next();
	
			String subject = sct.getSubject();
			String predicate = sct.getPredicate();
			Object obj = sct.getObject();
			
			// add the property
			predData.addProperty(predicate, predicate);

			// try to see if the predicate here has been designated to be a relation
			// if so then add it as a relation
			// if the object is a simple type
			// then it would be Predicate/SimpleType
			addProperty(subject, obj, predicate);
		}

	}
	
	public void genPropertiesLocal(String query2)
	{

		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		logger.debug("Executing Query " + query2);
		
		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);
		sjsc.setQuery(query2);
		sjsc.execute();
		
		while(sjsc.hasNext())
		{
			SesameJenaConstructStatement sct = sjsc.next();
	
			String subject = sct.getSubject();
			String predicate = sct.getPredicate();
			Object obj = sct.getObject();
			
			// add the property
			predData.addProperty(predicate, predicate);

			// try to see if the predicate here has been designated to be a relation
			// if so then add it as a relation
			// if the object is a simple type
			// then it would be Predicate/SimpleType
			addProperty(subject, obj, predicate);
		}
	}
	
	// removes existing concepts 
	public void removeExistingConcepts(String subjectsRemoved)
	{
		// run the query to find
		// I need to find the complete concept hierarchy and then wipe each one of it out
		// interesting
		String query2 = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
		//"VALUES ?Subject {" + subjectsRemoved+ " }" +
		"{?Subject  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Concept/System>;}" +
		//"{?Subject ?Predicate <http://health.mil/ontologies/dbcm/Concept>;}" +
		"{?Subject ?Predicate ?Object.}" +
		"}";
		
		IEngine jenaEngine = new InMemoryJenaEngine();
		((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);
		
		logger.warn("Executing Query " + query2);
		
		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);
		sjsc.setQuery(query2);
		sjsc.execute();
		
		Vector <SesameJenaConstructStatement> stAggregator = new Vector<SesameJenaConstructStatement>();
		while(sjsc.hasNext())
			stAggregator.addElement(sjsc.next());

		for(int stIdx = 0;stIdx < stAggregator.size();stIdx++)
		{
			removeFromJenaModel(stAggregator.elementAt(stIdx));
		}
	}
	
	// removes existing concepts 
	public void addNewConcepts(Hashtable <String, SesameJenaConstructStatement> subjectAddHash)
	{

		Enumeration keys = subjectAddHash.keys();
		while(keys.hasMoreElements())
		{
			addToJenaModel2(subjectAddHash.get(keys.nextElement()), false);
		}
	}
	
	// converts a predicate into 
	public void convertPredicateToProperty(String predicate)
	{
		
	}
	
	public void convertPropertyToPredicate(String predicate)
	{
		
	}
	

	// not sure if anyone uses this.. 
	// this can be killed I think
	private void extendForest() throws Exception {
		// this will extend it
		// i.e. Checks to see if the node is available
		// if the node is not already there then this predicate wont be added

		logger.info("Extending Forest >>>>>");

		Properties rdfMap = DIHelper.getInstance().getRdfMap();


		createBaseURIs();
		// iterate through the graph query result and set everything up
		// this is also the place where the vertex filter data needs to be created

		while (sjw.hasNext()) {
			SesameJenaConstructStatement st = sjw.next();

			String predicate = st.getPredicate();
			//writeStatus("Adding predicate " + predicate);
			predData.addPredicate(predicate);
			// need to work on this logic
			logger.debug(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
			if (Utility.checkPatternInString(RELATION_URI, predicate)
					&& !rdfMap.contains(predicate)) // need to change this to starts with relation
			{
				// only if the node is there on the checker
				// what if it comes in later ?
				// you cant predict the ordinality of the query
				// so there has to be some way to keep it
				// basically what if something is connected to a node which is connected to something else
				// however the something else comes in earlier
				// TODO currently assumes single level - try to do for multi level later
				if (Utility.checkPatternInString(PROP_URI, predicate)) {
					addProperty(st.getSubject(), st.getObject(), predicate);
				} else {
					String vert1Name = Utility.getInstanceName(st.getSubject());
					String vert2Name = Utility.getInstanceName(st.getObject()+"");
					// System.out.println("other routine");
					if (filterData.checker.containsKey(st.getSubject())
							|| filterData.checker.containsKey(st.getObject()+"")) {
						DBCMVertex vert1 = vertStore.get(st.getSubject());
						DBCMVertex vert2 = vertStore.get(st.getObject()+"");
						logger.debug("Found Edge " + st.getPredicate());
						if (vertStore.get(st.getSubject()) == null) {
							vert1 = new DBCMVertex(st.getSubject());
							vertStore.put(st.getSubject(), vert1);
						}
						if (vertStore.get(st.getObject()+"") == null) {
							vert2 = new DBCMVertex(st.getObject()+"");
							vertStore.put(st.getObject()+"", vert2);
						}
						genControlData(vert1);
						genControlData(vert2);
						try {
							DBCMEdge edge = edgeStore.get(st.getPredicate());
							if (edge == null) {
								edge = new DBCMEdge(vert1, vert2,
										st.getPredicate());
								edgeStore.put(st.getPredicate(), edge);
								genControlData(edge);
							}
							else
							{
								DBCMVertex inVert = edge.inVertex;
								DBCMVertex outVert = edge.outVertex;								
								String inURI = inVert.getURI();
								String outURI = outVert.getURI();
								
								if( !(st.getSubject().equalsIgnoreCase(inURI) && (st.getObject()+"").equalsIgnoreCase(outURI)) &&  !(st.getSubject().equalsIgnoreCase(outURI) && (st.getObject()+"").equalsIgnoreCase(inURI)))
								{
									// this is a random edge that needs to be taken care of
									// so add this with a different name
									String predicateName = st.getPredicate()+"/" + vert1.getProperty(Constants.VERTEX_NAME) + "-" + vert2.getProperty(Constants.VERTEX_NAME);
									edge = new DBCMEdge(vert1, vert2, predicateName);
									edgeStore.put(predicateName, edge);
									genControlData(edge);
								}								
							}
							// add the edge to filter data
							filterData.addEdge(edge);
							// I am going to replace this whole with refine forest and let it go
							
							// need to revisit this IF statement, ideally I will not need this if I take the lowest
							// level
							// need to see why this shit goes on
							// check to see if this node is already on the filter nodes then dont add it
							// if the vert1 is is in filterNodes - then dont add it
							// if the vert 2 is in filter node - then dont add it / show it
							if((filterData.checker.containsKey(st.getSubject()) || filterData.checker.containsKey(st.getObject()+"")))
							{
								filterData.addVertex(vert1);
								filterData.addVertex(vert2);
								// check to see if they are not in the filter nodes
								if(!filterData.filterNodes.containsKey(st.getSubject()) && !filterData.filterNodes.containsKey(st.getObject()+"") && !filterData.edgeFilterNodes.containsKey(st.getPredicate()))
								{
									graph.addVertex(vertStore.get(st.getSubject()));
									graph.addVertex(vertStore.get(st.getObject()+""));
									
									graph.addEdge(vertStore.get(st.getSubject()),
											vertStore.get(st.getObject()+""), edge);

									logger.debug("Adding new Edge " + st.getPredicate());
									this.forest.addEdge(edge,
										vertStore.get(st.getSubject()),
										vertStore.get(st.getObject()+""));
								}
							}
						}
						catch (Exception ignored) {
						}
					//addToJenaModel(st);
					}
				}
			}
		}
		
		// test call
		//refineForest();
		genAllData();
		logger.info("Extending Forest Complete >>>>>> ");
	}
	
	protected void genControlData(DBCMVertex vert1)
	{
		controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Constants.VERTEX_TYPE);
		controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Constants.VERTEX_NAME);							
		controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Constants.URI);									
	}
	
	protected void genControlData(DBCMEdge edge)
	{
		controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Constants.EDGE_TYPE);
		controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Constants.EDGE_NAME);							
		controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Constants.URI);							
		
	}
	
	protected void createBaseURIs()
	{
		RELATION_URI = DIHelper.getInstance().getProperty(
				Constants.PREDICATE_URI);
		PROP_URI = DIHelper.getInstance()
				.getProperty(Constants.PROP_URI);

	}
	
	public void setDataLatencyPopUp(JInternalFrame dataLate){
		dataLatencyPopUp = dataLate;
	}

	// this process is to remove an existing  forest
	// will come back to this later
	// this is going to be tricky
	// I need to remove the nodes not only from the forest.. but I need to realign the whole vertex filter data as well
	// I bet I will come back to this after the engines
	
	
	private void addProperty(String subject, Object object, String predicate) {
		
		
		// need to see here again if the subject is also a type of predicate
		// if it is then I need to get edge
		// else I need to get vertex
		if(!Utility.checkPatternInString(RELATION_URI, subject))
		{
			logger.debug("Creating property for a vertex" );
			DBCMVertex vert1 = vertStore.get(subject);
			if (vert1 == null) {
				vert1 = new DBCMVertex(subject);
			}
			vertStore.put(subject, vert1);
			vert1.setProperty(predicate, object);
			vertStore.put(subject, vert1);
			genControlData(vert1);
			//controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Utility.getClassName(predicate));
		}else
		{
			logger.debug("Creating property for an edge");
			if (subject.contains("Alert-Notification"))
			{
				String tom = "tom";
			}
			DBCMEdge edge = edgeStore.get(subject);
			
			if(edge == null)
			{
				logger.debug("Seems like an edge came up without having any vertices, the query is out of whack !!");
			}
			else{
				edge.setProperty(predicate, object);
				edgeStore.put(subject, edge);
				genControlData(edge);
				//controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Utility.getClassName(predicate));
			}
		}
	}
	
	protected void addToJenaModel2(SesameJenaConstructStatement st, boolean overrideURI) {
		// if the jena model is not null
		// then add to the new jenaModel and the old one
		try {
			if(rc == null)
			{
				Repository myRepository = new SailRepository(
			            new ForwardChainingRDFSInferencer(
			            new MemoryStore()));
				myRepository.initialize();
				
				rc = myRepository.getConnection();				
			}
			// undo
			if(curModel == null && (extend || append))
			{
				logger.info("Creating the new model");
				Repository myRepository2 = new SailRepository(
			            new ForwardChainingRDFSInferencer(
			            new MemoryStore()));
				myRepository2.initialize();
				
				curRC = myRepository2.getConnection();
				curModel = ModelFactory.createDefaultModel();
			}

			org.openrdf.model.Resource subject = new URIImpl(st.getSubject());
			org.openrdf.model.URI predicate = new URIImpl(st.getPredicate());
			
			// figure out if this is an object later
			Object obj = st.getObject();
			if((overrideURI || obj instanceof URI) && !(obj instanceof com.hp.hpl.jena.rdf.model.Literal))
			{
				org.openrdf.model.Resource object = null;
				if(obj instanceof org.openrdf.model.Resource)
				 object = (org.openrdf.model.Resource) obj;
				else 
					object = new URIImpl(st.getObject()+"");
				rc.add(subject,predicate,object);
				if(extend || append)
				{
					//logger.info("Adding to the new model");
					curRC.add(subject,predicate,object);
				}
			}
			else if(obj instanceof com.bigdata.rdf.model.BigdataValueImpl){
				rc.add(subject, predicate, (com.bigdata.rdf.model.BigdataValueImpl) obj);
				if(extend || append)
				{
					//logger.info("Adding to the new model");
					curRC.add(subject,predicate,rc.getValueFactory().createLiteral(obj+""));
				}
			}
			else{
				rc.add(subject, predicate, rc.getValueFactory().createLiteral(obj+""));
				if(extend || append)
				{
					//logger.info("Adding to the new model");
					curRC.add(subject,predicate,rc.getValueFactory().createLiteral(obj+""));
				}
			}
			
			

			
		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*jenaModel.add(jenaSt);*/
		// just so that we can remove it later
	}

	protected void addToJenaModel3(SesameJenaConstructStatement st) {
		// if the jena model is not null
		// then add to the new jenaModel and the old one
		if(jenaModel == null)
		{
			//jenaModel = ModelFactory.createDefaultModel(ReificationStyle.Standard);
			//Model baseModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MICRO_RULE_INF);
			Model baseModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_RULE_INF);
			jenaModel = ModelFactory.createInfModel(ReasonerRegistry.getTransitiveReasoner(),baseModel);	
			
		}
		Resource subject = jenaModel.createResource(st.getSubject());
		Property prop = jenaModel.createProperty(st.getPredicate());
		Resource object = jenaModel.createResource(st.getObject()+"");
		com.hp.hpl.jena.rdf.model.Statement jenaSt = null;
		//logger.warn("Adding Statement " + subject + "<>" + prop + "<>" + object);

		jenaSt = jenaModel.createStatement(subject, prop, object);
		/*
		if ((st.getObject()+"").contains("double"))
		{
			Double val = new Double(((Literal)st.getObject()).doubleValue());
			com.hp.hpl.jena.rdf.model.Literal l = ModelFactory.createDefaultModel().createTypedLiteral(val);
			jenaSt = jenaModel.createLiteralStatement(subject, prop, l);
			jenaModel.add(jenaSt);
			
		}
		else
		{
		
			
			jenaModel.add(jenaSt);
		}
		*/
		jenaModel.add(jenaSt);
		// just so that we can remove it later
		if(curModel == null && (extend || append))
		{
			logger.info("Creating the new model");
			curModel = ModelFactory.createDefaultModel();
		}
		if(extend || append)
		{
			//logger.info("Adding to the new model");
			curModel.add(jenaSt);
		}
	}

	
	protected void removeFromJenaModel(SesameJenaConstructStatement st) {
		Resource subject = jenaModel.createResource(st.getSubject());
		Property prop = jenaModel.createProperty(st.getPredicate());
		Resource object = jenaModel.createResource(st.getObject()+"");
		com.hp.hpl.jena.rdf.model.Statement jenaSt = null;

		logger.warn("Removing Statement " + subject + "<>" + prop + "<>" + object);
		jenaSt = jenaModel.createStatement(subject, prop, object);
		jenaModel.remove(jenaSt);
	}

	public void genAllData()
	{
		filterData.fillRows();
		filterData.fillEdgeRows();
		controlData.generateAllRows();
		predData.genPredList();
		colorShapeData.setTypeHash(filterData.typeHash);
		colorShapeData.setCount(filterData.count);
		colorShapeData.fillRows();
	}
	
	public SesameJenaConstructWrapper getSjw()
	{
		return sjw;
	}
	
	public void setSjw(SesameJenaConstructWrapper sjw)
	{
		this.sjw = sjw;
	}
	
	public VertexFilterData getFilterData() {
		return filterData;
	}

	public VertexColorShapeData getColorShapeData() {
		return colorShapeData;
	}

	public ControlData getControlData() {
		return controlData;
	}

	public PropertySpecData getPredicateData() {
		return predData;
	}
	
	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		logger.info("New Query " + query);
		this.query = query;
	}

	public ParamPanel getPanel() {
		return panel;
	}

	public void setParamPanel(ParamPanel panel) {
		this.panel = panel;
	}

	public DelegateForest getForest() {
		forest = new DelegateForest();
		graph = new SimpleGraph<DBCMVertex, DBCMEdge>(DBCMEdge.class);
		return forest;
	}

	public void setForest(DelegateForest forest) {
		this.forest = forest;
	}
	

	public void setLayout(String layout) {
		this.layoutName = layout;
	}

	public IEngine getRDFEngine() {
		return this.engine;
	}

	@Override
	public void run() {
		createView();

	}
	
	public Graph getGraph()
	{
		return graph;
	}
	
	public VisualizationViewer getView()
	{
		return view;
	}
	
	protected void printConnectedNodes()
	{
		logger.info("In print connected Nodes routine " );
		ConnectivityInspector ins = new ConnectivityInspector(graph);
		logger.info("Number of vertices " + graph.vertexSet().size() + "<>" + graph.edgeSet().size());
		logger.info(" Graph Connected ? " + ins.isGraphConnected());
		//writeStatus("Graph Connected ? " + ins.isGraphConnected());
		logger.info("Number of connected sets are " + ins.connectedSets().size());
		Iterator <Set<DBCMVertex>> csIterator = ins.connectedSets().iterator();
		int count = 0;
		while(csIterator.hasNext())
		{
			Set <DBCMVertex> vertSet = csIterator.next();
			Iterator <DBCMVertex> si = vertSet.iterator();
			while(si.hasNext())
			{
				DBCMVertex vert = si.next();
				//logger.info("Set " + count + ">>>> " + vert.getProperty(Constants.VERTEX_NAME));
			}
			count++;
		}	
	}	
	
	protected void printSpanningTree()
	{
		logger.info("In Spanning Tree " );
		KruskalMinimumSpanningTree<DBCMVertex, DBCMEdge> ins = new KruskalMinimumSpanningTree<DBCMVertex, DBCMEdge>(graph);
		
		logger.info("Number of vertices " + graph.vertexSet().size());
		logger.info(" Edges  " + ins.getEdgeSet().size());
		Iterator <DBCMEdge> csIterator = ins.getEdgeSet().iterator();
		int count = 0;
		while(csIterator.hasNext())
		{
				DBCMEdge vert = csIterator.next();
				//writeStatus("Set " + count + ">>>> " + vert.getProperty(Constants.EDGE_NAME));
				//logger.info("Set " + count + ">>>> " + vert.getProperty(Constants.EDGE_NAME));
		}
		count++;
	}	
	
	public void progressBarUpdate(String status, int x)
	{
		jBar.setString(status);
		jBar.setValue(x);

	}	

	
	public Model getJenaModel()
	{
		Model newModel = jenaModel;
		return newModel;
	}
	
	public void setJenaModel(Model jenaModel)
	{
		this.jenaModel=jenaModel;
	}
	
	public void setRC(RepositoryConnection rc)
	{
		this.rc=rc;
	}
	
	public VertexLabelFontTransformer getVertexLabelFontTransformer(){
		return vlft;
	}
	
	public void setActiveSheet()
	{
		try {
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void resetTransformers(){
		
		if(searchPanel.btnHighlight.isSelected()){
			SearchEdgeStrokeTransformer tx = (SearchEdgeStrokeTransformer)view.getRenderContext().getEdgeStrokeTransformer();
			tx.setEdges(null);
			SearchVertexPaintTransformer ptx = (SearchVertexPaintTransformer)view.getRenderContext().getVertexFillPaintTransformer();
			Hashtable searchVertices = searchPanel.resHash;
			ptx.setVertHash(searchVertices);
			SearchVertexLabelFontTransformer vfl = (SearchVertexLabelFontTransformer)view.getRenderContext().getVertexFontTransformer();
			vfl.setVertHash(searchVertices);
			EdgeArrowStrokeTransformer east = (EdgeArrowStrokeTransformer)view.getRenderContext().getEdgeArrowStrokeTransformer();
			east.setEdges(null);
		}
		else{
			EdgeStrokeTransformer tx = (EdgeStrokeTransformer)view.getRenderContext().getEdgeStrokeTransformer();
			tx.setEdges(null);
			VertexPaintTransformer ptx = (VertexPaintTransformer)view.getRenderContext().getVertexFillPaintTransformer();
			ptx.setVertHash(null);
			VertexLabelFontTransformer vfl = (VertexLabelFontTransformer)view.getRenderContext().getVertexFontTransformer();
			vfl.setVertHash(null);
			ArrowDrawPaintTransformer atx = (ArrowDrawPaintTransformer)view.getRenderContext().getArrowDrawPaintTransformer();
			atx.setEdges(null);
			EdgeArrowStrokeTransformer east = (EdgeArrowStrokeTransformer)view.getRenderContext().getEdgeArrowStrokeTransformer();
			east.setEdges(null);
			
		}
	}
	
	// removes existing concepts 
	public void removeExistingConcepts(Vector <String> subVector)
	{

		for(int remIndex = 0;remIndex < subVector.size();remIndex++)
		{
			try {
				String remQuery = subVector.elementAt(remIndex);
				logger.warn("Removing query " + remQuery);
				
				Update update = rc.prepareUpdate(QueryLanguage.SPARQL, remQuery);
				update.execute();
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	// adds existing concepts 
	public void addNewConcepts(String subjects, String baseObject, String predicate)
	{
		
		StringTokenizer tokenz = new StringTokenizer(subjects, ";");
		
		while(tokenz.hasMoreTokens())
		{
			String adder = tokenz.nextToken();
			
			String parent = adder.substring(0,adder.indexOf("@@"));
			String child = adder.substring(adder.indexOf("@@") + 2);
			
			SesameJenaConstructStatement st = new SesameJenaConstructStatement();
			st.setSubject(child);
			st.setPredicate(predicate);
			st.setObject(baseObject);
			addToJenaModel2(st,true);
			
			System.out.println(" Query....  " + parent + "<>" + child);	
		}
	}

	
	public void createBaseRelations(Properties rdfMap, String relationName) throws Exception{
		if(rdfMap.containsKey(relationName)){ //load using what is on the map
			String value = rdfMap.getProperty(relationName);
			System.out.println(" Relations are " + value);
			StringTokenizer relTokens = new StringTokenizer(value, ";");
			while (relTokens.hasMoreTokens()) {
				String rel = relTokens.nextToken();
				String relNames = rdfMap.getProperty(rel);
				StringTokenizer rdfTokens = new StringTokenizer(relNames, ";");
				int count = 0;
				while (rdfTokens.hasMoreTokens()) {
					StringTokenizer stmtTokens = new StringTokenizer(
							rdfTokens.nextToken(), "+");
					String subject = stmtTokens.nextToken();
					String predicate = stmtTokens.nextToken();
					String object = stmtTokens.nextToken();
					SesameJenaConstructStatement stmt = new SesameJenaConstructStatement();
					stmt.setSubject(subject);
					stmt.setPredicate(predicate);
					stmt.setObject(object);
					baseFilterHash.put(subject, subject);
					baseFilterHash.put(object+"", object+"");
					baseFilterHash.put(predicate, predicate);
					
					// create the statement now
					//addToJenaModel(stmt);
					addToJenaModel2(stmt, true);
	
				}// statement while
			}// relationship while
		}//if using map
	}
	/*public void queryPropAll() {
		String query = "SELECT ?subject ?predicate ?object WHERE {?predicate <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://health.mil/ontologies/dbcm/Relation/Contains>. ?subject ?predicate ?object. ?subject <http://www.w3.org/2000/01/rdf-schema#label> ?name;} BINDINGS ?name {@FILTER_VALUES@}";
		Hashtable<String, String> hash = new Hashtable<String, String>();
		
		//get nodes first
		
		Hashtable nodeHash = filterData.typeHash;
	    Enumeration hashE = nodeHash.keys();
		String fileName = "";
		int count = 0;
	    while (hashE.hasMoreElements()){
	    	String key = (String) hashE.nextElement();
	    	Vector <DBCMVertex> vertVector= (Vector<DBCMVertex>) nodeHash.get(key);
			for(int vertIndex = 0;vertIndex < vertVector.size();vertIndex++)
			{
				if(count == 0)
					fileName ="(\"" + Utility.getInstanceName(vertVector.elementAt(vertIndex).getURI()) + "\")";
				else
					fileName = fileName + "(\"" + Utility.getInstanceName(vertVector.elementAt(vertIndex).getURI()) + "\")";
				count++;
			}
	    }
	}*/
}
