package prerna.ui.components;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.beans.PropertyVetoException;
import java.lang.reflect.Constructor;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import javax.swing.JDesktopPane;
import javax.swing.JFrame;
import javax.swing.JInternalFrame;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;

import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.KruskalMinimumSpanningTree;
import org.jgrapht.graph.SimpleGraph;

import prerna.om.DBCMEdge;
import prerna.om.DBCMVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
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

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;

import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateForest;
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
	VisualizationViewer <DBCMVertex, DBCMEdge> view = null;
	String layoutName = Constants.FR;
	Layout layout2Use = null;
	JTabbedPane rightView = null;
	protected String title = null;
	protected IEngine engine = null;
	protected Model jenaModel = null;
	protected Model curModel = null;
	public LegendPanel2 legendPanel = null;
	public JPanel cheaterPanel = new JPanel();
	public JProgressBar jBar = new JProgressBar();
	public JTabbedPane jTab = new JTabbedPane();
	public Vector edgeVector = new Vector();
	public JInternalFrame dataLatencyPopUp = null;

	
	protected SimpleGraph <DBCMVertex, DBCMEdge> graph = new SimpleGraph<DBCMVertex, DBCMEdge>(DBCMEdge.class);
	
	public VertexFilterData filterData = new VertexFilterData();
	ControlData controlData = new ControlData();
	PropertySpecData predData = new PropertySpecData();
	VertexColorShapeData colorShapeData = new VertexColorShapeData();
	protected String questionNum = null;
	JDesktopPane pane = null;
	
	protected boolean append = false;
	protected boolean extend = false;
	
	// references to main vertstore
	protected Hashtable<String, DBCMVertex> vertStore = null;
	// references to the main edgeStore
	protected Hashtable<String, DBCMEdge> edgeStore = null;
	
	protected Properties rdfMap = null;
	protected String RELATION_URI = null;
	protected String PROP_URI = null;
	protected Logger logger = Logger.getLogger(getClass());
	public SearchPanel searchPanel = new SearchPanel();
	
	
	protected Vector <Model> modelStore = new Vector<Model>();

	public GraphPlaySheet()
	{
		super("_", true, true, true, true);
		
		
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
			addToMainPane();
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
			addToMainPane();
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
			queryPropAll();
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
			showAll();
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
				createForest();
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

			append = true;

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
			queryPropAll();
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
			logger.debug("Extend : Total Models added = " + modelStore.size());

			createLayout();
			// identify the layout specified for this perspective
			// now create the visualization viewer and we are done
			createVisualizer();
			// add the panel
			addPanel();
			// addpane
			// show it
			//showAll();
			this.setSelected(false);
			this.setSelected(true);
			printConnectedNodes();
			printSpanningTree();
			queryPropAll();
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
	

	public void undoView()
	{
		// get the latest and undo it
		if(modelStore.size() > 0)
		{
			Model lastModel = modelStore.elementAt(modelStore.size() - 1);
			// remove it from jena model
			logger.info("Number of new statements " + lastModel.size());
			logger.info("Number of statements in the old model " + jenaModel.size());
			StmtIterator stmti = lastModel.listStatements();
			while(stmti.hasNext())
			{
				jenaModel.remove(stmti.next());
			}
			progressBarUpdate("20%...Querying RDF Repository", 20);
			
			modelStore.remove(modelStore.size() - 1);
			
			refineView();
		}
		progressBarUpdate("100%...Graph Undo Complete", 100);
	}
	
	public void refineView() {
		// need to include the relation vs. prop logic

		logger.info("Refining Forest >>>>>");
		// refine the forest and then repaint
		// if I had someway to capture the edge this might not be that difficult
		// such life
		// need to be able to redo the whole graph here just as create forest
		getForest();
		createBaseURIs();
		// may be just recreate a new forest from the old one
		SesameJenaConstructWrapper sjw = new SesameJenaConstructWrapper();
		progressBarUpdate("40%...Querying RDF Repository", 40);
		sjw.setModel(jenaModel);
		sjw.setEngineType(IEngine.ENGINE_TYPE.JENA);
		
		Hashtable<String, String> filteredNodes = filterData.filterNodes;
		progressBarUpdate("60%...Processing RDF Statements	", 60);
		while (sjw.hasNext()) {
			// System.out.println("Iterating ...");
			SesameJenaConstructStatement st = sjw.next();

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
					DBCMVertex vert1 = vertStore.get(st.getSubject());
					DBCMVertex vert2 = vertStore.get(st.getObject()+"");
					DBCMEdge edge = edgeStore.get(predicate);
					
					// need to do the same routine as before and try it
					if(edge != null)
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
							edge = edgeStore.get(predicateName);
							//edgeStore.put(predicateName, edge);
						}
					}
					
					try {
						if (!vertStore.contains(st.getSubject() + "_"
								+ st.getObject())
								&& !vertStore.contains(st.getPredicate()))
							graph.addVertex(vertStore.get(st.getSubject()));
						graph.addVertex(vertStore.get(st.getObject()+""));
						
						graph.addEdge(vertStore.get(st.getSubject()),
								vertStore.get(st.getObject()+""), edge);
						
							
							this.forest.addEdge(edge, vert1, vert2);
							
							
					} catch (Exception ignored) {
					}
				}
				addToJenaModel(st);
			}
		}
		progressBarUpdate("80%...Creating Visualization", 80);
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
		
		showAll();
		progressBarUpdate("100%...Graph Refine Complete", 100);
	}
	public void refreshView(){
		createVisualizer();
		// add the panel
		addPanel();
		
		showAll();
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
			addToMainPane();
			showAll();

			/*
			if(query.toUpperCase().contains("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();
			 */
			//writeStatus(" Created the queries ");

			progressBarUpdate("10%...Querying RDF Repository", 10);
			//sjw.setQuery(query);
			//sjw.execute();
			
			sjw = new SesameJenaConstructWrapper();
			sjw.setModel(jenaModel);
			progressBarUpdate("30%...Querying RDF Repository", 30);
			sjw.setEngineType(IEngine.ENGINE_TYPE.JENA);
			progressBarUpdate("60%...Processing RDF Statements	", 60);
			//sjw.execute();
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
	
	public void setJDesktopPane(JDesktopPane pane)
	{
		this.pane = pane;
	}
	
	protected void addInitialPanel()
	{
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
		GridBagConstraints gbc_jBar = new GridBagConstraints();
		gbc_jBar.anchor = GridBagConstraints.NORTH;
		gbc_jBar.fill = GridBagConstraints.HORIZONTAL;
		gbc_jBar.gridx = 0;
		gbc_jBar.gridy = 1;
		cheaterPanel.add(jBar, gbc_jBar);
		
		this.getContentPane().setPreferredSize(new Dimension(800,600));
		this.getContentPane().setLayout(new BorderLayout());
		this.getContentPane().add(cheaterPanel, BorderLayout.PAGE_END);
		


	}
	
	protected void addPanel() {
		
		// add the model to search panel
		searchPanel.indexStatements(jenaModel);
		
		// create the listener and add the frame
		// JInternalFrame frame = new JInternalFrame(title, true, true, true, true);
		// frame.setPreferredSize(new Dimension(400,600));
		// if there is a view remove it
		// get
		Component[] comps = getContentPane().getComponents();
		for (int compIndex = 0; compIndex < comps.length; compIndex++) {
			logger.debug("Component is " + comps[compIndex]);
			if (comps[compIndex] instanceof JScrollPane) {
				logger.debug("Removing the component");
				remove(comps[compIndex]);
			}
		}
		JPanel testPanel = new JPanel();
		if (jTab.getTabCount()>0)
		{
			jTab.remove(0);
		}
		JPanel graphPanel = new JPanel();
		graphPanel.setLayout(new BorderLayout());
		graphPanel.add(searchPanel, BorderLayout.NORTH);
		graphPanel.add(view, BorderLayout.CENTER);
		jTab.insertTab("Graph", null, graphPanel, null, 0);
		jTab.setSelectedIndex(0);
		testPanel.setLayout(new BorderLayout());
		
		//testPanel.add(new SearchPanel(), BorderLayout.NORTH);
		
		testPanel.add(jTab, BorderLayout.CENTER);
		//testPanel.add(new JButton("Hula Hoop"), BorderLayout.PAGE_END);
		//testPanel.add(new JTextArea("Hula Hoop"), BorderLayout.PAGE_END);
		
		JScrollPane myPane = new JScrollPane(testPanel);
		myPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		//myPane.setViewportView(view);
		myPane.setAutoscrolls(true);
		//testPanel.add(myPane);
		this.getContentPane().add(myPane);
		this.setAutoscrolls(true);
		this.addInternalFrameListener(PlaySheetListener.getInstance());
		this.addInternalFrameListener(PlaySheetControlListener.getInstance());
		this.addInternalFrameListener(PlaySheetOWLListener.getInstance());
		this.addInternalFrameListener(PlaySheetColorShapeListener.getInstance());
		legendPanel.data = filterData;
		legendPanel.drawLegend();
		logger.info("Add Panel Complete >>>>>");
	}

	protected void addToMainPane() {
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(
				Constants.DESKTOP_PANE);
		pane.add(this);

		logger.info("Adding Main Panel Complete");
	}

	public void showAll() {
		this.pack();
		this.setVisible(true);
		JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
				Constants.MAIN_FRAME);
		frame2.repaint();

	}

	protected void createVisualizer() {
		view = new VisualizationViewer(this.layout2Use);
		
		Renderer r = new BasicRenderer();
		
		/*
		 * TODO
		 * final class MyRenderer implements Renderer.Vertex<DBCMVertex, DBCMEdge> {
		    @Override public void paintVertex(RenderContext<DBCMVertex, DBCMEdge> rc,
		        Layout<DBCMVertex, DBCMEdge> layout, DBCMVertex vertex) {
		      GraphicsDecorator graphicsContext = rc.getGraphicsContext();
		      Point2D center = layout.transform(vertex);
		      Shape shape = null;
		      Color color = null;
				float dash[] = {10.0f};
		        Stroke s = new BasicStroke(3.0f, BasicStroke.CAP_BUTT,
		                BasicStroke.JOIN_MITER, 10.0f, dash, 0.0f);
		      if(vertex.equals("Square")) {
		        shape = new Rectangle((int)center.getX()-10, (int)center.getY()-10, 20, 20);
		        color = new Color(127, 127, 0);
		      } else if(vertex.equals("Rectangle")) {
		        shape = new Rectangle((int)center.getX()-10, (int)center.getY()-20, 20, 40);
		        color = new Color(127, 0, 127);
		      } else if(vertex.equals("Circle")){}
		      {
		        shape = new Ellipse2D.Double(center.getX()-10, center.getY()-10, 20, 20);
		        color = new Color(0, 127, 127);
		      }
		      graphicsContext.setPaint(Color.BLACK);
		      graphicsContext.setStroke(s);
		      graphicsContext.setPaint(color);
		      graphicsContext.fill(shape);
		    }
		  };
		*/
		
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
		VertexLabelFontTransformer vlft = new VertexLabelFontTransformer();
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
	
	protected void createForest() throws Exception {
		logger.info("Creating Forest >>>>>");

		Properties rdfMap = DIHelper.getInstance().getRdfMap();

		createBaseURIs();
		// this will also create a jena model
		// iterate through the graph query result and set everything up
		// this is also the place where the vertex filter data needs to be created
		String [] status = new String[]{"/", "\\", "--", "|"};
		
		logger.debug(" Adding graph to forest " );
		int count = 0;
		while (sjw.hasNext()) {
			logger.debug("Iterating ...");
			SesameJenaConstructStatement st = sjw.next();

			String predicate = st.getPredicate();
			predData.addPredicate(predicate);
			// need to work on this logic
			logger.debug(st.getSubject() + "<>" + st.getPredicate() + "<>" + st.getObject());
			if (Utility.checkPatternInString(RELATION_URI, predicate)
					&& !rdfMap.contains(predicate)) // need to change this to starts with relation
			{
				if (Utility.checkPatternInString(PROP_URI, predicate)) {
					logger.debug("Add Property Routine");
					addProperty(st.getSubject(), st.getObject(), predicate);
				} else {
					logger.debug("Create edge routine");
					DBCMVertex vert1 = vertStore.get(st.getSubject());
					
					// there is a good possibility that the 
					logger.debug("Adding Edge " + st.getPredicate());
					if (vertStore.get(st.getSubject()) == null) {
						vert1 = new DBCMVertex(st.getSubject());
						vertStore.put(st.getSubject(), vert1);
					}
					DBCMVertex vert2 = vertStore.get(st.getObject()+"");
					if (vertStore.get(st.getObject()+"") == null) {
						vert2 = new DBCMVertex(st.getObject()+"");
						vertStore.put(st.getObject() +"", vert2);
					}
					genControlData(vert1);
					genControlData(vert2);
					filterData.addVertex(vert1);
					filterData.addVertex(vert2);
					try {
						DBCMEdge edge = edgeStore.get(st.getPredicate());
						if (edge == null) {
							edge = new DBCMEdge(vert1, vert2, st.getPredicate());
							edgeStore.put(st.getPredicate(), edge);
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
							}
							
						}
						// add the edge to filter data
						filterData.addEdge(edge);
						
						// need to revisit this IF statement, ideally I will not need this if I take the lowest level
						// need to see why this shit goes on
						if (!vertStore.contains(st.getSubject() + "_"
								+ st.getObject())
								&& !vertStore.contains(st.getPredicate()))
						{
							this.forest.addEdge(edge,
									vertStore.get(st.getSubject()),
									vertStore.get(st.getObject()+""));
							genControlData(edge);
							
							// to be removed later
							graph.addVertex(vertStore.get(st.getSubject()));
							graph.addVertex(vertStore.get(st.getObject()+""));
							
							graph.addEdge(vertStore.get(st.getSubject()),
									vertStore.get(st.getObject()+""), edge);
							
							//logger.info("Edge added is " + edgeT);
							//logger.info("Number of edges so far " + graph.vertexSet().size());

						}
					} catch (Exception ignored) {
						//logger.warn("Exception " + ignored);
						//ignored.printStackTrace();
					}
				}
				addToJenaModel(st);
			}
			else
				addToJenaModel(st);
		}
		genAllData();
		logger.info("Creating Forest Complete >>>>>> ");
	}

	
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
					addToJenaModel(st);
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
				logger.warn("Seems like an edge came up without having any vertices, the query is out of whack !!");
			}
			else{
				edge.setProperty(predicate, object);
				edgeStore.put(subject, edge);
				genControlData(edge);
				//controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Utility.getClassName(predicate));
			}
		}
	}
	public String queryPropAll() {
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
	    
	    //get edges
		nodeHash = filterData.edgeTypeHash;
	    hashE = nodeHash.keys();

	    while (hashE.hasMoreElements()){
	    	String key = (String) hashE.nextElement();
	    	Vector <DBCMEdge> edgeVector= (Vector <DBCMEdge>) nodeHash.get(key);
			for(int edgeIndex = 0;edgeIndex < edgeVector.size();edgeIndex++)
			{
				fileName = fileName + "(\"" + Utility.getInstanceName(edgeVector.elementAt(edgeIndex).getURI()) + "\")";
			}
	    }
	
		hash.put("FILTER_VALUES", fileName);
		String filledQuery = Utility.fillParam(query, hash);
		extendProp(filledQuery);
		return filledQuery;
	}
	protected void addToJenaModel(SesameJenaConstructStatement st) {
		// if the jena model is not null
		// then add to the new jenaModel and the old one
		if(jenaModel == null)
			jenaModel = ModelFactory.createDefaultModel();
		Resource subject = jenaModel.createResource(st.getSubject());
		Property prop = jenaModel.createProperty(st.getPredicate());
		com.hp.hpl.jena.rdf.model.Statement jenaSt = null;

		jenaSt = jenaModel.createStatement(subject, prop, st.getObject()+"");
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
	
}
