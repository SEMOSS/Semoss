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
package prerna.ui.components.playsheets;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import javax.swing.JButton;
import javax.swing.JComponent;
import javax.swing.JInternalFrame;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.Painter;
import javax.swing.UIDefaults;
import javax.swing.UIManager;

import org.jgrapht.Graph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.alg.KruskalMinimumSpanningTree;
import org.jgrapht.graph.SimpleGraph;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.InMemoryJenaEngine;
import prerna.rdf.engine.impl.InMemorySesameEngine;
import prerna.rdf.engine.impl.RDFFileSesameEngine;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;
import prerna.rdf.engine.impl.SesameJenaConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaSelectCheater;
import prerna.rdf.engine.impl.SesameJenaUpdateWrapper;
import prerna.rdf.util.SPARQLParse;
import prerna.ui.components.ControlData;
import prerna.ui.components.ControlPanel;
import prerna.ui.components.GraphOWLHelper;
import prerna.ui.components.LegendPanel2;
import prerna.ui.components.NewHoriScrollBarUI;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.PropertySpecData;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.components.VertexColorShapeData;
import prerna.ui.components.VertexFilterData;
import prerna.ui.components.specific.tap.DataLatencyPlayPopup;
import prerna.ui.main.listener.impl.GraphNodeListener;
import prerna.ui.main.listener.impl.PickedStateListener;
import prerna.ui.main.listener.impl.PlaySheetColorShapeListener;
import prerna.ui.main.listener.impl.PlaySheetControlListener;
import prerna.ui.main.listener.impl.PlaySheetListener;
import prerna.ui.main.listener.impl.PlaySheetOWLListener;
import prerna.ui.swing.custom.ProgressPainter;
import prerna.ui.transformer.ArrowDrawPaintTransformer;
import prerna.ui.transformer.ArrowFillPaintTransformer;
import prerna.ui.transformer.EdgeArrowStrokeTransformer;
import prerna.ui.transformer.EdgeLabelFontTransformer;
import prerna.ui.transformer.EdgeLabelTransformer;
import prerna.ui.transformer.EdgeStrokeTransformer;
import prerna.ui.transformer.EdgeTooltipTransformer;
import prerna.ui.transformer.VertexIconTransformer;
import prerna.ui.transformer.VertexLabelFontTransformer;
import prerna.ui.transformer.VertexLabelTransformer;
import prerna.ui.transformer.VertexPaintTransformer;
import prerna.ui.transformer.VertexShapeTransformer;
import prerna.ui.transformer.VertexStrokeTransformer;
import prerna.ui.transformer.VertexTooltipTransformer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.JenaSesameUtils;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;

import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateForest;
import edu.uci.ics.jung.visualization.GraphZoomScrollPane;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.picking.PickedState;
import edu.uci.ics.jung.visualization.renderers.BasicRenderer;
import edu.uci.ics.jung.visualization.renderers.Renderer;

/**
 */
public class GraphPlaySheet extends AbstractRDFPlaySheet {

	/*
	 * this will have references to the following a. Internal Frame that needs to be displayed b. The panel of
	 * parameters c. The composed SPARQL Query d. Perspective selected e. The question selected by the user f. Filter
	 * criterias including slider values
	 */
	protected SesameJenaConstructWrapper sjw = null;
	public DelegateForest forest = null;
	protected Hashtable <String, String> loadedOWLS = new Hashtable<String, String>();
	public VisualizationViewer <SEMOSSVertex, SEMOSSEdge> view = null;
	String layoutName = Constants.FR;
	Layout layout2Use = null;
	public LegendPanel2 legendPanel = null;
	public JPanel cheaterPanel = new JPanel();
	public JTabbedPane jTab = new JTabbedPane();
	public Vector edgeVector = new Vector();
	public JInternalFrame dataLatencyPopUp = null;
	public DataLatencyPlayPopup dataLatencyPlayPopUp = null;
	
	//So that it doesn't get reset on extend and overlay etc. it must be stored
	VertexLabelFontTransformer vlft;
	EdgeLabelFontTransformer elft;
	VertexShapeTransformer vsht;

	
	protected SimpleGraph <SEMOSSVertex, SEMOSSEdge> graph = new SimpleGraph<SEMOSSVertex, SEMOSSEdge>(SEMOSSEdge.class);
	
	public VertexFilterData filterData = new VertexFilterData();
	ControlData controlData = new ControlData();
	PropertySpecData predData = new PropertySpecData();

	VertexColorShapeData colorShapeData = new VertexColorShapeData();
	
	// references to main vertstore
	public Hashtable<String, SEMOSSVertex> vertStore = null;
	// references to the main edgeStore
	public Hashtable<String, SEMOSSEdge> edgeStore = null;
	// checks to see if we already added a particular set of vertifces
	// if so tracks it as the same edge
	
	protected Properties rdfMap = null;
	protected String RELATION_URI = null;
	protected String PROP_URI = null;
	public ControlPanel searchPanel;
	public RepositoryConnection rc = null;
	protected RepositoryConnection curRC = null;
	protected RDFFileSesameEngine baseRelEngine = null;
	public Hashtable baseFilterHash = new Hashtable();
	protected Model jenaModel = null;
	protected Model curModel = null;
	public int modelCounter = 0;
	protected Vector <Model> modelStore = new Vector<Model>();
	protected Vector <RepositoryConnection> rcStore = new Vector<RepositoryConnection>();
	
	public boolean sudowl, search, prop;
	public JSplitPane graphSplitPane;

	/**
	 * Constructor for GraphPlaySheet.
	 */
	public GraphPlaySheet()
	{
		
		vertStore = new Hashtable<String, SEMOSSVertex>();
		edgeStore =new Hashtable<String, SEMOSSEdge>();
		rdfMap = DIHelper.getInstance().getRdfMap();
		createBaseURIs();
		logger.info("Graph PlaySheet " + query);
	}
	
	/**
	 * Method setAppend.
	 * @param append boolean
	 */
	public void setAppend(boolean append) {
		logger.debug("Append set to " + append);
		//writeStatus("Append set to  : " + append);
		this.overlay = append;
	}
	
	/**
	 * Method setExtend.
	 * @param extend boolean
	 */
	public void setExtend(boolean extend)
	{
		logger.debug("Extend set to " + extend);
		//writeStatus("Extend set to  : " + extend);
		this.extend = extend;
	}

	/**
	 * Method createView.
	 */
	public void createView() {
		if(rc==null){
			String questionID = getQuestionID();
			// fill the nodetype list so that they can choose from
			// remove from store
			// this will also clear out active sheet
			QuestionPlaySheetStore.getInstance().remove(questionID);
			if(QuestionPlaySheetStore.getInstance().isEmpty())
			{
				JButton btnShowPlaySheetsList = (JButton) DIHelper.getInstance().getLocalProp(
						Constants.SHOW_PLAYSHEETS_LIST);
				btnShowPlaySheetsList.setEnabled(false);
			}
			Utility.showError("Query returned no results.");
			return;
		}
		super.createView();
		
		this.setPreferredSize(new Dimension(1000,750));

		searchPanel=new ControlPanel(search);

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
			
			
			addInitialPanel();

			addToMainPane(pane);
			showAll();
			/*if(queryCap.startsWith("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();

			//writeStatus(" Created the queries ");

			sjw.setEngine(engine);
			updateProgressBar("10%...Querying RDF Repository", 10);
			sjw.setQuery(query);
			updateProgressBar("30%...Querying RDF Repository", 30);
			try{
				sjw.execute();	
			}
			catch (Exception e)
			{
				UIDefaults nimbusOverrides = new UIDefaults();
				UIDefaults defaults = UIManager.getLookAndFeelDefaults();
				defaults.put("nimbusOrange",defaults.get("nimbusInfoBlue"));
				Painter red = new ProgressPainter(Color.WHITE, Color.RED);
				nimbusOverrides.put("ProgressBar[Enabled].foregroundPainter",red);
				jBar.putClientProperty("Nimbus.Overrides", nimbusOverrides);
				jBar.putClientProperty("Nimbus.Overrides.InheritDefaults", false);
				updateProgressBar("An error has occurred. Please check the query.", 100);
				return;
			}*/		
				
			updateProgressBar("60%...Processing RDF Statements	", 60);
			
			logger.debug("Executed the select");
			createForest();
			createLayout();
			processView();
			updateProgressBar("100%...Graph Generation Complete", 100);
			
		} catch (Exception e) {
			e.printStackTrace();
			logger.fatal(e.getStackTrace());
		}
	}
	
	/**
	 * Method processView.
	 */
	public void processView()
	{
		
		createVisualizer();
		updateProgressBar("80%...Creating Visualization", 80);
		
		addPanel();
		try {
			this.setSelected(false);
			this.setSelected(true);
			printConnectedNodes();
			printSpanningTree();
			//logger.debug("model size: " +rc.size());
		} catch (Exception e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}
	
	/**
	 * Method extendView.
	 */
	public void extendView()
	{
		try
		{
			rc.commit();
			extend = true;
			//getForest();
			curModel = null;
			/*
			String queryCap = query.toUpperCase();
			if(queryCap.startsWith("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();
			sjw.setEngine(engine);
			updateProgressBar("10%...Querying RDF Repository", 10);
			sjw.setQuery(query);
			updateProgressBar("30%...Querying RDF Repository", 30);
			sjw.execute();
			*/
			extend = true;
			createForest();
			
			//add to overall modelstore
			modelStore.addElement(curModel);
			rcStore.addElement(curRC);
			
			boolean successfulLayout = createLayout();
			if(!successfulLayout){
				Utility.showMessage("Current layout cannot handle the extend. Resetting to " + Constants.FR + " layout...");
				layoutName = Constants.FR;
				createLayout();
			}
			
			processView();
			processTraverseCourse();
			updateProgressBar("100%...Graph Extension Complete", 100);
		}catch(Exception ex)
		{
			ex.printStackTrace();
			logger.fatal(ex);
		}
	}
	
	/**
	 * Method overlayView.
	 */
	public void overlayView()
	{
		try {
			extend = false;

			overlay = true;

			curModel = null;

			String queryCap = query.toUpperCase();
			if(queryCap.startsWith("CONSTRUCT"))
				sjw = new SesameJenaConstructWrapper();
			else
				sjw = new SesameJenaSelectCheater();
			
			sjw.setEngine(engine);
			updateProgressBar("10%...Querying RDF Repository", 10);
			sjw.setQuery(query);
			updateProgressBar("30%...Querying RDF Repository", 30);
			sjw.execute();
			updateProgressBar("60%...Processing RDF Statements	", 60);
			
			createForest();
			updateProgressBar("80%...Creating Visualization", 80);
			
			//add to overall modelstore
			modelStore.addElement(curModel);
			rcStore.addElement(curRC);
			
			boolean successfulLayout = createLayout();
			if(!successfulLayout){
				Utility.showMessage("Current layout cannot handle the overlay. Resetting to " + Constants.FR + " layout...");
				layoutName = Constants.FR;
				createLayout();
			}
			
			processView();
			processTraverseCourse();
			updateProgressBar("100%...Graph Extension Complete", 100);
		} catch (Exception e) {
			// TODO: Specify exception
			logger.fatal(e);
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Method processTraverseCourse.
	 */
	public void processTraverseCourse()
	{
		//if you're at a spot where you have forward models, extensions will reset the future, thus we need to remove all future models
		//modelCounter already added by the time it gets here so you need to -1 to modelCounter
		if (rcStore.size()>=modelCounter-1)
		{
			//have to start removing from teh back of the model to avoid the rcstore from resizing
			//
			for (int modelIdx=rcStore.size()-1;modelIdx>=modelCounter-2;modelIdx--)
			{
				modelStore.remove(modelIdx);
				rcStore.remove(modelIdx);
			}
		}
		modelStore.addElement(curModel);
		rcStore.addElement(curRC);
		logger.debug("Extend : Total Models added = " + modelStore.size());
		setUndoRedoBtn();
	}
	
	
	/**
	 * Method undoView.
	 */
	public void undoView()
	{
		// get the latest and undo it
		// Need to find a way to keep the base relationships
		try {
			if(modelCounter > 1)
			{
				updateProgressBar("30%...Getting Previous Model", 30);
				RepositoryConnection lastRC = rcStore.elementAt(modelCounter-2);
				Model lastModel = modelStore.elementAt(modelCounter-2);
				// remove undo model from repository connection
				logger.info("Number of undo statements " + lastRC.size());
				logger.info("Number of statements in the old model " + rc.size());
				IEngine sesameEngine = new InMemorySesameEngine();
				((InMemorySesameEngine)sesameEngine).setRepositoryConnection(lastRC);
				RDFEngineHelper.removeAllData(sesameEngine, rc);
				//jenaModel.remove(lastModel);
				modelCounter--;
				
				filterData = new VertexFilterData();
				controlData = new ControlData();
				predData = new PropertySpecData();
				vertStore = new Hashtable<String, SEMOSSVertex>();
				edgeStore = new Hashtable<String, SEMOSSEdge>();
				updateProgressBar("50%...Graph Undo in Progress", 50);
				
				refineView();
				logger.info("model size: " +rc.size());
			}
			this.setSelected(false);
			this.setSelected(true);
			printConnectedNodes();
			printSpanningTree();

			genAllData();
		} catch (RepositoryException e) {
			e.printStackTrace();
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		updateProgressBar("100%...Graph Undo Complete", 100);
	}

	
    /**
     * Method redoView.
     */
    public void redoView() {
        try {
               if(rcStore.size() > modelCounter-1)
               {
                     updateProgressBar("30%...Getting Previous Model", 30);
                     RepositoryConnection newRC = rcStore.elementAt(modelCounter-1);
                     Model newModel = modelStore.elementAt(modelCounter-1);
                     //add redo model from repository connection
                      logger.info("Number of redo statements " + newRC.size());
                     logger.info("Number of statements in the old model " + rc.size());
                     
                     IEngine sesameEngine = new InMemorySesameEngine();
                     ((InMemorySesameEngine)sesameEngine).setRepositoryConnection(newRC);
                     RDFEngineHelper.addAllData(sesameEngine, rc);
                     //jenaModel.add(newModel);
                     modelCounter++;
                     updateProgressBar("50%...Graph Redo in Progress", 50);
                     refineView();
                     genAllData();
                     
               }
               this.setSelected(false);
               this.setSelected(true);
               printConnectedNodes();
               printSpanningTree();
               genAllData();
        } catch (RepositoryException e){
            e.printStackTrace();
        } catch (PropertyVetoException e) {
        	e.printStackTrace();
        }
        updateProgressBar("100%...Graph Redo Complete", 100);
    }


	
	/**
	 * Method removeView.
	 */
	public void removeView()
	{
		// this will extend it
		// i.e. Checks to see if the node is available
		// if the node is not already there then this predicate wont be added

		String queryCap = query.toUpperCase();
		if(queryCap.startsWith("CONSTRUCT"))
			sjw = new SesameJenaConstructWrapper();
		else
			sjw = new SesameJenaSelectCheater();
		sjw.setEngine(engine);
		sjw.setQuery(query);
		sjw.execute();

		Model curModel = ModelFactory.createDefaultModel();
		
		while (sjw.hasNext()) {
			SesameJenaConstructStatement st = sjw.next();
			org.openrdf.model.Resource subject = new URIImpl(st.getSubject());
			org.openrdf.model.URI predicate = new URIImpl(st.getPredicate());
			String delQuery = "DELETE DATA {";
			// figure out if this is an object later
			Object obj = st.getObject();
			delQuery=delQuery+"<"+subject+"><"+predicate+">";
	
			if((obj instanceof com.hp.hpl.jena.rdf.model.Literal) || (obj instanceof Literal))
			{
	
				delQuery=delQuery+obj+".";
			}
			else 
			{
				delQuery=delQuery+"<"+obj+">";
			}
			//delQuery = "DELETE DATA {<http://health.mil/ontologies/Concept/System/CHCS><http://semoss.org/ontologies/Relation/Provide><http://health.mil/ontologies/Concept/InterfaceControlDocument/CHCS-ABTS-Order_Information>}";
			delQuery = delQuery+"}";
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
			delQuery = delQuery+".";
			//count++;
			logger.debug(delQuery);
		}
			

		//sc.addStatement(vf.createURI("<http://semoss.org/ontologies/Concept/Service/tom2>"),vf.createURI("<http://semoss.org/ontologies/Relation/Exposes>"),vf.createURI("<http://semoss.org/ontologies/Concept/BusinessLogicUnit/tom1>"));
		logger.debug("\nSPARQL: " + query);
		//tq.setIncludeInferred(true /* includeInferred */);
		//tq.evaluate();

		genBaseGraph();
		updateProgressBar("80%...Creating Visualization", 80);
		//writeStatus("Total Statements Dropped " + count);
		// test call

		refineView();
		logger.debug("Removing Forest Complete >>>>>> ");
		updateProgressBar("100%...Graph Remove Complete", 100);
	}
	

	
	/**
	 * Method refineView.
	 */
	public void refineView()
	{
		try {
			getForest();
			genBaseConcepts();
			genBaseGraph();
			//progressBarUpdate("80%...Creating Visualization", 80);
			
			String containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";
			
			// now that this is done, we can query for concepts
			String propertyQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
			  "{?Predicate " +"<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> " +  containsRelation + ";}" +
			  //"{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
			  		"{?Subject ?Predicate ?Object}}";					

			RDFEngineHelper.genNodePropertiesLocal(rc, containsRelation, this);
			RDFEngineHelper.genEdgePropertiesLocal(rc, containsRelation, this);

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
			setUndoRedoBtn();
		} catch (Exception e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
	}
	

	/**
	 * Method refreshView.
	 */
	public void refreshView(){
		createVisualizer();
		// add the panel
		addPanel();
		try {
			this.setSelected(false);
			this.setSelected(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
		
		//showAll();
	}

	/**
	 * Method addInitialPanel.
	 */
	public void addInitialPanel()
	{
		setWindow();
		// create the listener and add the frame
		// JInternalFrame frame = new JInternalFrame(title, true, true, true, true);
		// frame.setPreferredSize(new Dimension(400,600));
		// if there is a view remove it
		// get
		
		this.addInternalFrameListener(PlaySheetListener.getInstance());
		this.addInternalFrameListener(PlaySheetControlListener.getInstance());
		this.addInternalFrameListener(PlaySheetOWLListener.getInstance());
		this.addInternalFrameListener(PlaySheetColorShapeListener.getInstance());

		
		GridBagLayout gridBagLayout = new GridBagLayout();
		gridBagLayout.columnWidths = new int[]{728, 0};
		gridBagLayout.rowHeights = new int[]{557, 70, 0};
		gridBagLayout.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gridBagLayout.rowWeights = new double[]{1.0, 0.0, Double.MIN_VALUE};
		getContentPane().setLayout(gridBagLayout);

	
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
		resetProgressBar();
       
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
		graphSplitPane = new JSplitPane();

		graphSplitPane.setEnabled(false);
		graphSplitPane.setOneTouchExpandable(true);
		graphSplitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
		searchPanel.setPlaySheet(this);

		

	}
	
	/**
	 * Method addPanel.
	 */
	protected void addPanel() {
		try
		{
			// add the model to search panel
			if (search)
			{
				searchPanel.searchCon.indexStatements(jenaModel);
			}
			//graphSplitPane.removeAll();
			//graphPanel.setLayout(new BorderLayout());
			GraphZoomScrollPane gzPane = new GraphZoomScrollPane(view);
			gzPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
			gzPane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
//			GridBagLayout gbl_graphPanel = new GridBagLayout();
//			gbl_graphPanel.columnWidths = new int[]{0, 0};
//			gbl_graphPanel.rowHeights = new int[]{0, 0, 0};
//			gbl_graphPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
//			gbl_graphPanel.rowWeights = new double[]{0.0, 1.0, Double.MIN_VALUE};
//			graphPanel.setLayout(gbl_graphPanel);
//			
//			GridBagConstraints gbc_search = new GridBagConstraints();
//			gbc_search.insets = new Insets(0, 0, 5, 0);
//			gbc_search.fill = GridBagConstraints.BOTH;
//			gbc_search.gridx = 0;
//			gbc_search.gridy = 0;
			graphSplitPane.setLeftComponent(searchPanel);
			
//			GridBagConstraints gbc_panel_2 = new GridBagConstraints();
//			gbc_panel_2.fill = GridBagConstraints.BOTH;
//			gbc_panel_2.gridx = 0;
//			gbc_panel_2.gridy = 1;
			graphSplitPane.setRightComponent(gzPane);	
			
			this.addComponentListener(
					new ComponentAdapter(){
						public void componentResized(ComponentEvent e){
							logger.info(((JInternalFrame)e.getComponent()).isMaximum());
							GraphPlaySheet gps = (GraphPlaySheet) e.getSource();
							if(!layoutName.equals(Constants.TREE_LAYOUT))
								layout2Use.setSize(view.getSize());
							logger.info("Size: " + gps.view.getSize());
							
						}
					});
	
			legendPanel.data = filterData;
			legendPanel.drawLegend();
			logger.info("Adding graph tab");
			boolean setSelected = jTab.getSelectedIndex()==0;
			jTab.insertTab("Graph", null, graphSplitPane, null, 0);
			if(setSelected) jTab.setSelectedIndex(0);
			logger.info("Add Panel Complete >>>>>");
		}catch(Exception ex)
		{
			
		}
	}

	/**
	 * Method addToMainPane.
	 * @param pane JComponent
	 */
	protected void addToMainPane(JComponent pane) {

		pane.add((Component)this);

		logger.info("Adding Main Panel Complete");
	}

	/**
	 * Method showAll.
	 */
	public void showAll() {
		this.pack();
		this.setVisible(true);
		//JFrame frame2 = (JFrame) DIHelper.getInstance().getLocalProp(
	//			Constants.MAIN_FRAME);
		//frame2.repaint();

	}

	/**
	 * Method createVisualizer.
	 */
	protected void createVisualizer() {
		//tree layout cannot set size
		if(!layoutName.equals(Constants.TREE_LAYOUT))
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
		VertexTooltipTransformer vtt = new VertexTooltipTransformer(controlData);
		EdgeLabelTransformer elt = new EdgeLabelTransformer(controlData);
		EdgeTooltipTransformer ett = new EdgeTooltipTransformer(controlData);
		EdgeStrokeTransformer est = new EdgeStrokeTransformer();
		VertexStrokeTransformer vst = new VertexStrokeTransformer();
		ArrowDrawPaintTransformer adpt = new ArrowDrawPaintTransformer();
		EdgeArrowStrokeTransformer east = new EdgeArrowStrokeTransformer();
		ArrowFillPaintTransformer aft = new ArrowFillPaintTransformer();
		PickedStateListener psl = new PickedStateListener(view);
		//keep the stored one if possible
		if(vlft==null)
			vlft = new VertexLabelFontTransformer();
		if(elft==null)
			elft = new EdgeLabelFontTransformer();
		if(vsht==null)
			vsht = new VertexShapeTransformer();
		else vsht.emptySelected();
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
		view.getRenderContext().setEdgeFontTransformer(elft);
		view.getRenderer().getVertexLabelRenderer().setPosition(Renderer.VertexLabel.Position.CNTR);
		view.getRenderContext().setLabelOffset(0);
		//view.getRenderContext().set;
		// view.getRenderContext().setVertexIconTransformer(new DBCMVertexIconTransformer());
		view.setVertexToolTipTransformer(vtt);
		view.setEdgeToolTipTransformer(ett);
		//view.getRenderContext().setVertexIconTransformer(vit);
		PickedState ps = view.getPickedVertexState();
		ps.addItemListener(psl);
		controlData.setViewer(view);
		searchPanel.setViewer(view);
		logger.info("Completed Visualization >>>> ");
	}

	/**
	 * Method createLayout.
	 * @return boolean
	 */
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
		searchPanel.setGraphLayout(layout2Use);
		//= (Layout) new FRLayout((forest));
		logger.info("Create layout Complete >>>>>> ");
		if(fail==2) {
			return false;
		}
		else return true;
	}
	
	/**
	 * Method getLayoutName.
	 * @return String
	 */
	public String getLayoutName(){
		return layoutName;
	}
	
	
	/**
	 * Method createForest.
	 */
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
		
		// replacing the current logic with SPARQLParse
		
		// I am going to use the same standard query
		/*String thisquery = "SELECT ?System1 ?Upstream ?ICD ?Downstream ?System2 ?carries ?Data1 ?contains2 ?prop2 ?System3 ?Upstream2 ?ICD2 ?contains1 ?prop ?Downstream2 ?carries2 ?Data2 ?Provide ?BLU" +
		" WHERE { {?System1  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System1){{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}{?System1 ?Upstream ?ICD ;}{?ICD ?Downstream ?System2 ;} {?ICD ?carries ?Data1;}{?carries ?contains2 ?prop2} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Upstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}  {?ICD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Data2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?carries2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?System3 ?Upstream2 ?ICD2 ;}{?ICD2 ?Downstream2 ?System1 ;} {?ICD2 ?carries2 ?Data2;} {?carries2 ?contains1 ?prop} {?contains1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System1 ?Provide ?BLU}}}";
		SPARQLParse parse = new SPARQLParse();
		parse.createRepository();
		parse.parseIt(thisquery);
		parse.executeQuery(thisquery, engine);
		parse.loadBaseDB(engine.getProperty(Constants.OWL));
		this.rc = parse.rc;
		*/
		
		// this is where the block goes
		
		/*
		boolean isError = false;
		if(rc != null && (extend || overlay))
		{
			logger.info("Creating the new model");
			Repository myRepository2 = new SailRepository(
		            new ForwardChainingRDFSInferencer(
		            new MemoryStore()));
			myRepository2.initialize();
			
			curRC = myRepository2.getConnection();
			curModel = ModelFactory.createDefaultModel();
		}
		StringBuffer subjects = new StringBuffer("");
		StringBuffer predicates = new StringBuffer("");
		StringBuffer objects = new StringBuffer("");
		if(!sjw.hasNext())
		{
			return;
		}
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
			//predData.addConceptAvailable(st.getSubject());//, st.getSubject());
			//predData.addPredicateAvailable(st.getPredicate());//, st.getPredicate());

			if(subjects.indexOf("(<" + st.getSubject() + ">)") < 0)
			{
				if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
					subjects.append("(<").append(st.getSubject()).append(">)");
				else
					subjects.append("<").append(st.getSubject()).append(">");
			}
			if(predicates.indexOf("(<" + st.getPredicate() +">)") < 0)
			{
				if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
					predicates.append("(<").append(st.getPredicate()).append(">)");
				else
					predicates.append("<").append(st.getPredicate()).append(">");
			}
			// need to find a way to do this for jena too
			if(obj instanceof URI && !(obj instanceof com.hp.hpl.jena.rdf.model.Literal))
			{			
				if(objects.indexOf("(<" + obj +">)") < 0)
				{
					if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
						objects.append("(<" + obj +">)");
					else
						objects.append("<" + obj +">");
				}
			}
			//addToJenaModel(st);
			addToSesame(st, false, false);
			if (search) addToJenaModel3(st);
		}			
		logger.debug("Subjects >>> " + subjects);
		logger.debug("Predicatss >>>> " + predicates);
		
		// now add the base relationships to the metamodel
		// this links the hierarchy that tool needs to the metamodel being queried
		// eventually this could be a SPIN
		// need to get the engine name and jam it - Done Baby
		if(!loadedOWLS.containsKey(engine.getEngineName()) && engine instanceof AbstractEngine)
		{
			if(this.baseRelEngine == null){
				this.baseRelEngine = ((AbstractEngine)engine).getBaseDataEngine();
			}
			else {
				RDFEngineHelper.addAllData(((AbstractEngine)engine).getBaseDataEngine(), this.baseRelEngine.getRC());
			}
			if (((AbstractEngine)engine).getBaseHash() == null)
			{
				String owlFile = (String) DIHelper.getInstance().getProperty(engine.getEngineName() + "_" + Constants.OWL);
				//engine.get
				this.baseFilterHash = RDFEngineHelper.loadBaseRelationsFromOWL(owlFile);
			}
			else
			{
				this.baseFilterHash.putAll(((AbstractEngine)engine).getBaseHash());
			}
			RDFEngineHelper.addAllData(baseRelEngine, rc);
			loadedOWLS.put(engine.getEngineName(), engine.getEngineName());
		}
		logger.info("BaseQuery");
		// load the concept linkages
		// the concept linkages are a combination of the base relationships and what is on the file
		boolean loadHierarchy = !(subjects.equals("") && predicates.equals("") && objects.equals("")); 
		if(loadHierarchy)
		{
			try
			{
				RDFEngineHelper.loadConceptHierarchy(engine, subjects.toString(), objects.toString(), this);
				logger.debug("Loaded Concept");
				RDFEngineHelper.loadRelationHierarchy(engine, predicates.toString(), this);
				logger.debug("Loaded Relation");
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}*/
		// then query the database for concepts
		// get all the concepts
		//subjects2 = findAllConcepts(subjects2);
		// then query this model for everything that is beginning with that
		logger.info("Creating the base Graph");
		genBaseConcepts();
		logger.info("Loaded Orphan");
		genBaseGraph();//subjects2, predicates2, subjects2);
		logger.info("Loaded Graph");
		
		//find the contains property
		// Need to do the properties piece shortly
		// done
		// get it a single shot
		// find the name of the properties relation
		/*
		String containsRelation = findContainsRelation();
		if(containsRelation == null)
			containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";


		
		try {
			//RDFEngineHelper.loadLabels(engine, subjects+objects, this);
		} catch (Exception e) {
			// TODO: Specify exception
			e.printStackTrace();
		}
		
		if(sudowl){
			logger.info("Starting to load OWL");
			GraphOWLHelper.loadConceptHierarchy(rc, subjects.toString(), objects.toString(), this);
			GraphOWLHelper.loadRelationHierarchy(rc, predicates.toString(), this);
			GraphOWLHelper.loadPropertyHierarchy(rc,predicates.toString(), containsRelation, this);
			logger.info("Finished loading OWL");
		}
		if(prop){
			logger.info("Starting to load properties");
			logger.info("Creating the properties");
			if(containsRelation != null)
			{
				// load local property hierarchy
				try
				{
					//loadPropertyHierarchy(predicates, containsRelation);
					RDFEngineHelper.loadPropertyHierarchy(engine,predicates.toString(), containsRelation, this);
					// now that this is done, we can query for concepts						
					//genPropertiesRemote(propertyQuery + "BINDINGS ?Subject { " + subjects + " " + predicates + " " + objects+ " } ");
					RDFEngineHelper.genPropertiesRemote(engine, subjects.toString(), objects.toString(), predicates.toString(), containsRelation, this);
					RDFEngineHelper.genNodePropertiesLocal(rc, containsRelation, this);
					RDFEngineHelper.genEdgePropertiesLocal(rc, containsRelation, this);
					logger.info("Loaded Properties");
				}catch(Exception ex)
				{
					ex.printStackTrace();
				}
				//genProperties(propertyQuery + predicates + " } ");
			}
			
			logger.debug("Finished loading properties");
		}*/
		genAllData();

		logger.info("Done with everything");
		// first execute all the predicate selectors
		// Backdoor entry
		Thread thread = new Thread(){
			public void run()
			{
				printAllRelationship();				
			}
		};
		thread.start();
		modelCounter++;
		logger.info("Creating Forest Complete >>>>>> ");										
	}
	
	/**
	 * Method exportDB.
	 */
	public void exportDB() 
	{
		try {
			baseRelEngine.exportDB();
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
	}	
    /**
     * Method printAllRelationship.
     */
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
          logger.warn("<<<<");
          String end = "";
          
                while(!end.equalsIgnoreCase("end"))
                {
                      try {
                      BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                      logger.debug("Enter Query");
                      String query2 = reader.readLine();                    
                      end = query2;
                      logger.debug("Query is " + query2);
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
                            // TODO: Specify exception
                            e.printStackTrace();
                      }
                }

          
    }

	
	/**
	 * Method findContainsRelation.
	 * @return String
	 */
	private String findContainsRelation()
	{
		String query2 = "SELECT DISTINCT ?Subject ?subProp ?contains WHERE { BIND( <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> AS ?subProp) BIND( <http://semoss.org/ontologies/Relation/Contains> AS ?contains) {?Subject ?subProp  ?contains}}";

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
	
	/**
	 * Method genBaseConcepts.
	 */
	public void genBaseConcepts()
	{
		// create all the relationships now
		String conceptSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
									  //"VALUES ?Subject {"  + subjects + "}"+
									  //"VALUES ?Object {"  + subjects + "}"+
									  //"VALUES ?Object {"  + objects + "}" +
									  //"VALUES ?Predicate {"  + predicates + "}" +
									  //"{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
									  "{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  //"{?Object " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  "{?Subject ?Predicate ?Object}" +
									  "}";
		
		logger.info("ConceptSelectQuery query " + conceptSelectQuery);
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);

		Hashtable<String, String> filteredNodes = filterData.filterNodes;
		logger.warn("Filtered Nodes " + filteredNodes);
				
		logger.debug(conceptSelectQuery);
		
		try {
			sjsc.setQuery(conceptSelectQuery);
			sjsc.execute();
			logger.debug("Execute complete");

			int count = 0;
			while(sjsc.hasNext())
			{
				//logger.debug("Iterating " + count);
				count++;

				SesameJenaConstructStatement sct = sjsc.next();

				if(!baseFilterHash.containsKey(sct.getSubject()))// && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
				{
						SEMOSSVertex vert1 = vertStore.get(sct.getSubject()+"");
						if(vert1 == null)
						{
							vert1 = new SEMOSSVertex(sct.getSubject());
							vertStore.put(sct.getSubject()+"", vert1);
							genControlData(vert1);
						}
						// add my friend
						if(filteredNodes == null || (filteredNodes != null && !filteredNodes.containsKey(sct.getSubject()+"")))
							this.forest.addVertex(vertStore.get(sct.getSubject()));
						filterData.addVertex(vert1);
				}
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	// executes the first SPARQL query and generates the graphs
	/**
	 * Method genBaseGraph.
	 */
	public void genBaseGraph()
	{
		// create all the relationships now
		String predicateSelectQuery = "SELECT DISTINCT ?Subject ?Predicate ?Object WHERE {" +
									  //"VALUES ?Subject {"  + subjects + "}"+
									  //"VALUES ?Object {"  + subjects + "}"+
									  //"VALUES ?Object {"  + objects + "}" +
									  //"VALUES ?Predicate {"  + predicates + "}" +
									  "{?Predicate " +"<http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>;}" +
									  "{?Subject " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  //"{?Object " + "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  " +  " <http://semoss.org/ontologies/Concept>;}" +
									  "{?Subject ?Predicate ?Object}" +
									  "}";
		
		
		//IEngine jenaEngine = new InMemoryJenaEngine();
		//((InMemoryJenaEngine)jenaEngine).setModel(jenaModel);

		IEngine jenaEngine = new InMemorySesameEngine();
		((InMemorySesameEngine)jenaEngine).setRepositoryConnection(rc);

		SesameJenaSelectCheater sjsc = new SesameJenaSelectCheater();
		sjsc.setEngine(jenaEngine);

		Hashtable<String, String> filteredNodes = filterData.filterNodes;
		logger.warn("Filtered Nodes " + filteredNodes);
				
		logger.debug(predicateSelectQuery);
		
		try {
			sjsc.setQuery(predicateSelectQuery);
			sjsc.execute();
			logger.warn("Execute compelete");

			int count = 0;
			while(sjsc.hasNext())
			{
				//logger.warn("Iterating " + count);
				count++;

				SesameJenaConstructStatement sct = sjsc.next();
				String predicateName = sct.getPredicate();
				
				if(!baseFilterHash.containsKey(sct.getSubject()) && !baseFilterHash.containsKey(sct.getPredicate()) && !baseFilterHash.containsKey(sct.getObject()+""))
				{
					// get the subject, predicate and object
					// look for the appropriate vertices etc and paint it
					predData.addConceptAvailable(sct.getSubject());
					predData.addConceptAvailable(sct.getObject()+"");
					SEMOSSVertex vert1 = vertStore.get(sct.getSubject()+"");
					if(vert1 == null)
					{
						vert1 = new SEMOSSVertex(sct.getSubject());
						vertStore.put(sct.getSubject()+"", vert1);
						genControlData(vert1);
					}
					SEMOSSVertex vert2 = vertStore.get(sct.getObject()+"");
					if(vert2 == null )//|| forest.getInEdges(vert2).size()>=1)
					{
						if(sct.getObject() instanceof URI)
							vert2 = new SEMOSSVertex(sct.getObject()+"");
						else // ok this is a literal
							vert2 = new SEMOSSVertex(sct.getPredicate(), sct.getObject());
						vertStore.put(sct.getObject()+"", vert2);
						genControlData(vert2);
					}
					// create the edge now
					SEMOSSEdge edge = edgeStore.get(sct.getPredicate()+"");
					// check to see if this is another type of edge
					if(sct.getPredicate().indexOf(vert1.getProperty(Constants.VERTEX_NAME)+"") < 0 && sct.getPredicate().indexOf(vert2.getProperty(Constants.VERTEX_NAME)+"") < 0)
						predicateName = sct.getPredicate() + "/" + vert1.getProperty(Constants.VERTEX_NAME) + ":" + vert2.getProperty(Constants.VERTEX_NAME);
					if(edge == null)
						edge = edgeStore.get(predicateName);
					if(edge == null)
					{
						// need to create the predicate at runtime I think
						/*edge = new DBCMEdge(vert1, vert2, sct.getPredicate());
						System.err.println("Predicate plugged is " + predicateName);
						edgeStore.put(sct.getPredicate()+"", edge);*/
	
						// the logic works only when the predicates dont have the vertices on it.. 
						edge = new SEMOSSEdge(vert1, vert2, predicateName);
						edgeStore.put(predicateName, edge);
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
							predData.addPredicateAvailable(sct.getPredicate());
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
						ex.printStackTrace();
						logger.warn("Missing Edge " + edge.getURI() + "<<>>" + vert1.getURI() + "<<>>" + vert2.getURI());
						// ok.. I am going to ignore for now that this is a duplicate edge
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}
	
	/**
	 * Method setUndoRedoBtn.
	 */
	private void setUndoRedoBtn()
	{
		if(modelCounter>1)
		{
			searchPanel.undoBtn.setEnabled(true);
		}
		else
		{
			searchPanel.undoBtn.setEnabled(false);
		}
		if(rcStore.size()>=modelCounter)
		{
			searchPanel.redoBtn.setEnabled(true);
		}
		else
		{
			searchPanel.redoBtn.setEnabled(false);
		}
	}
				
	// not sure if anyone uses this.. 
	// this can be killed I think
	
	/**
	 * Method genControlData.
	 * @param vert1 DBCMVertex
	 */
	protected void genControlData(SEMOSSVertex vert1)
	{
		controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Constants.VERTEX_TYPE);
		controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Constants.VERTEX_NAME);							
		controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Constants.URI);
										
	}
	
	/**
	 * Method genControlData.
	 * @param edge DBCMEdge
	 */
	protected void genControlData(SEMOSSEdge edge)
	{
		controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Constants.EDGE_TYPE);
		controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Constants.EDGE_NAME);							
		controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Constants.URI);	
		
	}
	
	/**
	 * Method createBaseURIs.
	 */
	protected void createBaseURIs()
	{
		RELATION_URI = DIHelper.getInstance().getProperty(
				Constants.PREDICATE_URI);
		PROP_URI = DIHelper.getInstance()
				.getProperty(Constants.PROP_URI);

	}
	
	/**
	 * Method setDataLatencyPopUp.
	 * @param dataLate JInternalFrame
	 */
	public void setDataLatencyPopUp(JInternalFrame dataLate){
		dataLatencyPopUp = dataLate;
	}

	/**
	 * Method setDataLatencyPlayPopUp.
	 * @param dataLate DataLatencyPlayPopup
	 */
	public void setDataLatencyPlayPopUp(DataLatencyPlayPopup dataLate){
		dataLatencyPlayPopUp = dataLate;
	}
	
	
	/**
	 * Method addNodeProperty.
	 * @param subject String
	 * @param object Object
	 * @param predicate String
	 */
	public void addNodeProperty(String subject, Object object, String predicate) {
		
		
		// need to see here again if the subject is also a type of predicate
		// if it is then I need to get edge
		// else I need to get vertex
			logger.debug("Creating property for a vertex" );
			SEMOSSVertex vert1 = vertStore.get(subject);
			if (vert1 == null) {
				vert1 = new SEMOSSVertex(subject);
				genControlData(vert1);
			}
			vert1.setProperty(predicate, object);
			vertStore.put(subject, vert1);
			controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Utility.getInstanceName(predicate));
//			genControlData(vert1);
			//controlData.addProperty(vert1.getProperty(Constants.VERTEX_TYPE)+"", Utility.getClassName(predicate));
	}
		
	/**
	 * Method addEdgeProperty.
	 * @param subject String
	 * @param object Object
	 * @param predicate String
	 */
	public void addEdgeProperty(String edgeName, Object value, String propName, String outNode, String inNode) {
		logger.debug("Creating property for an edge");
		SEMOSSEdge edge = edgeStore.get(edgeName);

		if(edge == null)
		{
			SEMOSSVertex vert1 = vertStore.get(outNode);
			if (vert1 == null) {
				vert1 = new SEMOSSVertex(outNode);
				genControlData(vert1);
				vertStore.put(outNode, vert1);
			}
			SEMOSSVertex vert2 = vertStore.get(inNode);
			if (vert2 == null) {
				vert2 = new SEMOSSVertex(inNode + "");
				genControlData(vert2);
				vertStore.put(inNode + "", vert2);
			}
			 edge = new SEMOSSEdge(vert1, vert2, edgeName);
		}
		edge.setProperty(propName, value);
		edgeStore.put(edgeName, edge);
		controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Utility.getInstanceName(propName));
//			genControlData(edge);
		//controlData.addProperty(edge.getProperty(Constants.EDGE_TYPE)+"", Utility.getClassName(predicate));
	}

	/**
	 * Method addToSesame.
	 * @param st SesameJenaConstructStatement
	 * @param overrideURI boolean
	 * @param add2Base boolean
	 */
	public void addToSesame(SesameJenaConstructStatement st, boolean overrideURI, boolean add2Base) {
		// if the jena model is not null
		// then add to the new jenaModel and the old one
		// TODO based on the base relations add to base
		try {
			
			// initialization routine...
			if(rc == null)
			{
				Repository myRepository = new SailRepository(
			            new ForwardChainingRDFSInferencer(
			            new MemoryStore()));
				myRepository.initialize();
				
				rc = myRepository.getConnection();	
				rc.setAutoCommit(false);
			}
			// undo

			
			// done Initialization
			
			// Create the subject and predicate
			
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
				
				if(extend || overlay)
				{
					//logger.info("Adding to the new model");
					if (!rc.hasStatement(subject,predicate,object, true))
					{
						curRC.add(subject,predicate,object);
					}
					else
					{
						return;
					}
				}
				if(add2Base)
				{
					baseRelEngine.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), true);
				}
				rc.add(subject,predicate,object);
			}
			// the else basically means a couple of things
			// this is not a URI would the primary
			else if(obj instanceof Literal) // all the sesame routine goes here
			{
				/*if(obj instanceof com.bigdata.rdf.model.BigdataValueImpl){
				rc.add(subject, predicate, (com.bigdata.rdf.model.BigdataValueImpl) obj);
				if(extend || overlay)
				{
					//logger.info("Adding to the new model");
					curRC.add(subject,predicate,rc.getValueFactory().createLiteral(obj+""));
				}
				if(add2Base)
				{
					baseRelEngine.addStatement(st.getSubject(), st.getPredicate(), obj, false);
				}*/
				
				if(extend || overlay)
				{
					//logger.info("Adding to the new model");
					if (!rc.hasStatement(subject,predicate,(Literal)obj, true))
					curRC.add(subject,predicate,(Literal)obj);
					else
					{
						return;
					}
				}
				if(add2Base)
				{
					baseRelEngine.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), false);
				}
				rc.add(subject, predicate, (Literal)obj);
			}
			else if(obj instanceof com.hp.hpl.jena.rdf.model.Literal)
			{
				// I need to figure out a way to convert this into sesame literal
				Literal newObj = JenaSesameUtils.asSesameLiteral((com.hp.hpl.jena.rdf.model.Literal)obj);
				System.err.println("Adding to sesame " + subject + predicate + rc.getValueFactory().createLiteral(obj+""));
				
				if(extend || overlay)
				{
					//logger.info("Adding to the new model");
					if (!rc.hasStatement(subject,predicate,(Literal)obj, true))
					curRC.add(subject,predicate,(Literal)newObj);
					else
					{
						return;
					}
				}
				if(add2Base)
				{
					baseRelEngine.addStatement(st.getSubject(), st.getPredicate(), st.getObject(), false);
				}
				rc.add(subject, predicate, (Literal)newObj);
			}
		} catch (RepositoryException e) {
			e.printStackTrace();
		}

		/*jenaModel.add(jenaSt);*/
		// just so that we can remove it later
	}

	/**
	 * Method addToJenaModel3.
	 * @param st SesameJenaConstructStatement
	 */
	public void addToJenaModel3(SesameJenaConstructStatement st) {
		// if the jena model is not null
		// then add to the new jenaModel and the old one
		if(jenaModel == null)
		{
			//jenaModel = ModelFactory.createDefaultModel(ReificationStyle.Standard);
			//Model baseModel = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM_MICRO_RULE_INF);
			//Model baseModel = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);
			jenaModel = ModelFactory.createDefaultModel();
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
		if (!jenaModel.contains(jenaSt))
		{
			jenaModel.add(jenaSt);
			if(extend || overlay)
			{
			
				//logger.info("Adding to the new model");
				curModel.add(jenaSt);
			}
		}
		//jenaModel.add(jenaSt);
		// just so that we can remove it later


	}

	
	/**
	 * Method removeFromJenaModel.
	 * @param st SesameJenaConstructStatement
	 */
	protected void removeFromJenaModel(SesameJenaConstructStatement st) {
		Resource subject = jenaModel.createResource(st.getSubject());
		Property prop = jenaModel.createProperty(st.getPredicate());
		Resource object = jenaModel.createResource(st.getObject()+"");
		com.hp.hpl.jena.rdf.model.Statement jenaSt = null;

		logger.warn("Removing Statement " + subject + "<>" + prop + "<>" + object);
		jenaSt = jenaModel.createStatement(subject, prop, object);
		jenaModel.remove(jenaSt);
	}

	/**
	 * Method genAllData.
	 */
	public void genAllData()
	{
		filterData.fillRows();
		filterData.fillEdgeRows();
		controlData.generateAllRows();
		if(sudowl)
			predData.genPredList();
		colorShapeData.setTypeHash(filterData.typeHash);
		colorShapeData.setCount(filterData.count);
		colorShapeData.fillRows();
	}
	
	/**
	 * Method getSjw.
	 * @return SesameJenaConstructWrapper
	 */
	public SesameJenaConstructWrapper getSjw()
	{
		return sjw;
	}
	
	/**
	 * Method setSjw.
	 * @param sjw SesameJenaConstructWrapper
	 */
	public void setSjw(SesameJenaConstructWrapper sjw)
	{
		this.sjw = sjw;
	}
	
	/**
	 * Method getFilterData.
	 * @return VertexFilterData
	 */
	public VertexFilterData getFilterData() {
		return filterData;
	}

	/**
	 * Method getColorShapeData.
	 * @return VertexColorShapeData
	 */
	public VertexColorShapeData getColorShapeData() {
		return colorShapeData;
	}

	/**
	 * Method getControlData.
	 * @return ControlData
	 */
	public ControlData getControlData() {
		return controlData;
	}

	/**
	 * Method getPredicateData.
	 * @return PropertySpecData
	 */
	public PropertySpecData getPredicateData() {
		return predData;
	}
	

	/**
	 * Method getForest.
	 * @return DelegateForest
	 */
	public DelegateForest getForest() {
		forest = new DelegateForest();
		graph = new SimpleGraph<SEMOSSVertex, SEMOSSEdge>(SEMOSSEdge.class);
		return forest;
	}

	/**
	 * Method setForest.
	 * @param forest DelegateForest
	 */
	public void setForest(DelegateForest forest) {
		this.forest = forest;
	}
	

	/**
	 * Method setLayout.
	 * @param layout String
	 */
	public void setLayout(String layout) {
		this.layoutName = layout;
	}

	/**
	 * Method getGraph.
	 * @return Graph
	 */
	public Graph getGraph()
	{
		return graph;
	}
	
	/**
	 * Method getView.
	 * @return VisualizationViewer
	 */
	public VisualizationViewer getView()
	{
		return view;
	}
	
	/**
	 * Method printConnectedNodes.
	 */
	protected void printConnectedNodes()
	{
		logger.info("In print connected Nodes routine " );
		ConnectivityInspector ins = new ConnectivityInspector(graph);
		logger.info("Number of vertices " + graph.vertexSet().size() + "<>" + graph.edgeSet().size());
		logger.info(" Graph Connected ? " + ins.isGraphConnected());
		//writeStatus("Graph Connected ? " + ins.isGraphConnected());
		logger.info("Number of connected sets are " + ins.connectedSets().size());
		Iterator <Set<SEMOSSVertex>> csIterator = ins.connectedSets().iterator();
		while(csIterator.hasNext())
		{
			Set <SEMOSSVertex> vertSet = csIterator.next();
			Iterator <SEMOSSVertex> si = vertSet.iterator();
			while(si.hasNext())
			{
				SEMOSSVertex vert = si.next();
				//logger.info("Set " + count + ">>>> " + vert.getProperty(Constants.VERTEX_NAME));
			}
		}	
	}	
	
	/**
	 * Method printSpanningTree.
	 */
	protected void printSpanningTree()
	{
		logger.info("In Spanning Tree " );
		KruskalMinimumSpanningTree<SEMOSSVertex, SEMOSSEdge> ins = new KruskalMinimumSpanningTree<SEMOSSVertex, SEMOSSEdge>(graph);
		
		logger.info("Number of vertices " + graph.vertexSet().size());
		logger.info(" Edges  " + ins.getEdgeSet().size());
		Iterator <SEMOSSEdge> csIterator = ins.getEdgeSet().iterator();
		int count = 0;
		while(csIterator.hasNext())
		{
				SEMOSSEdge vert = csIterator.next();
				logger.info("Set " + count + ">>>> " + vert.getProperty(Constants.EDGE_NAME));
		}
		count++;
	}	
	
	/**
	 * Method getJenaModel.
	 * @return Model
	 */
	public Model getJenaModel()
	{
		Model newModel = jenaModel;
		return newModel;
	}
	
	/**
	 * Method setJenaModel.
	 * @param jenaModel Model
	 */
	public void setJenaModel(Model jenaModel)
	{
		this.jenaModel=jenaModel;
	}
	
	/**
	 * Method setRC.
	 * @param rc RepositoryConnection
	 */
	public void setRC(RepositoryConnection rc)
	{
		this.rc=rc;
	}
	
	/**
	 * Method getRC.
	 * @param rc RepositoryConnection
	 */
	public RepositoryConnection getRC()
	{
		return rc;
	}
	
	/**
	 * Method getEdgeLabelFontTransformer.
	 * @return EdgeLabelFontTransformer
	 */
	public EdgeLabelFontTransformer getEdgeLabelFontTransformer(){
		return elft;
	}

	/**
	 * Method getVertexLabelFontTransformer.
	 * @return VertexLabelFontTransformer
	 */
	public VertexLabelFontTransformer getVertexLabelFontTransformer(){
		return vlft;
	}
	
	/**
	 * Method resetTransformers.
	 */
	public void resetTransformers(){

		EdgeStrokeTransformer tx = (EdgeStrokeTransformer)view.getRenderContext().getEdgeStrokeTransformer();
		tx.setEdges(null);
		ArrowDrawPaintTransformer atx = (ArrowDrawPaintTransformer)view.getRenderContext().getArrowDrawPaintTransformer();
		atx.setEdges(null);
		EdgeArrowStrokeTransformer east = (EdgeArrowStrokeTransformer)view.getRenderContext().getEdgeArrowStrokeTransformer();
		east.setEdges(null);
		VertexShapeTransformer vst = (VertexShapeTransformer)view.getRenderContext().getVertexShapeTransformer();
		vst.setVertexSizeHash(new Hashtable());
		
		if(searchPanel.btnHighlight.isSelected()){
			VertexPaintTransformer ptx = (VertexPaintTransformer)view.getRenderContext().getVertexFillPaintTransformer();
			Hashtable searchVertices = new Hashtable();
			searchVertices.putAll(searchPanel.searchCon.cleanResHash);
			ptx.setVertHash(searchVertices);
			VertexLabelFontTransformer vfl = (VertexLabelFontTransformer)view.getRenderContext().getVertexFontTransformer();
			vfl.setVertHash(searchVertices);
		}
		else{
			VertexPaintTransformer ptx = (VertexPaintTransformer)view.getRenderContext().getVertexFillPaintTransformer();
			ptx.setVertHash(null);
			VertexLabelFontTransformer vfl = (VertexLabelFontTransformer)view.getRenderContext().getVertexFontTransformer();
			vfl.setVertHash(null);
		}
	}
	
	// removes existing concepts 
	/**
	 * Method removeExistingConcepts.
	 * @param subVector Vector<String>
	 */
	public void removeExistingConcepts(Vector <String> subVector)
	{

		for(int remIndex = 0;remIndex < subVector.size();remIndex++)
		{
			try {
				String remQuery = subVector.elementAt(remIndex);
				logger.warn("Removing query " + remQuery);
				
				Update update = rc.prepareUpdate(QueryLanguage.SPARQL, remQuery);
				update.execute();
				this.baseRelEngine.execInsertQuery(remQuery);
			
			} catch (Exception e) {
				// TODO: Specify exception
				e.printStackTrace();
			}
		}
	}
	
	//update all internal models associated with this playsheet with the query passed in
	/**
	 * Method updateAllModels.
	 * @param query String
	 */
	public void updateAllModels(String query){
		logger.debug(query);
		
		// run query on rc
		try{
			rc.commit();
		}catch(Exception e){
			
		}
		InMemorySesameEngine rcSesameEngine = new InMemorySesameEngine();
		rcSesameEngine.setRepositoryConnection(rc);
		SesameJenaUpdateWrapper sjuw = new SesameJenaUpdateWrapper();
		sjuw.setEngine(rcSesameEngine);
		sjuw.setQuery(query);
		sjuw.execute();
		logger.info("Ran update against rc");

		// run query on curRc
		if(curRC != null){
			InMemorySesameEngine curRcSesameEngine = new InMemorySesameEngine();
			curRcSesameEngine.setRepositoryConnection(curRC);
			sjuw.setEngine(curRcSesameEngine);
			sjuw.setQuery(query);
			sjuw.execute();
			logger.info("Ran update against curRC");
		}

		// run query on jenaModel
		InMemoryJenaEngine modelJenaEngine = new InMemoryJenaEngine();
		modelJenaEngine.setModel(jenaModel);
		sjuw.setEngine(modelJenaEngine);
		sjuw.setQuery(query);
		sjuw.execute();
		logger.info("Ran update against jenaModel");

		// run query on jenaModel
		if (curModel!=null){
			InMemoryJenaEngine curModelJenaEngine = new InMemoryJenaEngine();
			curModelJenaEngine.setModel(curModel);
			sjuw.setEngine(curModelJenaEngine);
			sjuw.setQuery(query);
			sjuw.execute();
			logger.info("Ran update against curModel");
		}
	}
	
	// adds existing concepts 
	/**
	 * Method addNewConcepts.
	 * @param subjects String
	 * @param baseObject String
	 * @param predicate String
	 * @return String
	 */
	public String addNewConcepts(String subjects, String baseObject, String predicate)
	{
		
		StringTokenizer tokenz = new StringTokenizer(subjects, ";");
		
		String listOfChilds = null;
		
		while(tokenz.hasMoreTokens())
		{
			String adder = tokenz.nextToken();
			
			String parent = adder.substring(0,adder.indexOf("@@"));
			String child = adder.substring(adder.indexOf("@@") + 2);
			
			if(listOfChilds == null)
				listOfChilds = child;
			else
			listOfChilds = listOfChilds + ";" + child;
			
			SesameJenaConstructStatement st = new SesameJenaConstructStatement();
			st.setSubject(child);
			st.setPredicate(predicate);
			st.setObject(baseObject);
			addToSesame(st,true, true);
			
			logger.info(" Query....  " + parent + "<>" + child);	
		}
		return listOfChilds;
	}

	@Override
	public void createData() {
		
		// open up the engine
		String queryCap = query.toUpperCase();

		if(queryCap.startsWith("CONSTRUCT"))
			sjw = new SesameJenaConstructWrapper();
		else
			sjw = new SesameJenaSelectCheater();

		//writeStatus(" Created the queries ");

		sjw.setEngine(engine);
		updateProgressBar("10%...Querying RDF Repository", 10);
		sjw.setQuery(query);
		updateProgressBar("30%...Querying RDF Repository", 30);
		try{
			sjw.execute();	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		
		logger.info("Executed the query");
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
		
		// replacing the current logic with SPARQLParse
		
		// I am going to use the same standard query
		/*String thisquery = "SELECT ?System1 ?Upstream ?ICD ?Downstream ?System2 ?carries ?Data1 ?contains2 ?prop2 ?System3 ?Upstream2 ?ICD2 ?contains1 ?prop ?Downstream2 ?carries2 ?Data2 ?Provide ?BLU" +
		" WHERE { {?System1  <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} BIND(<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System1){{?System2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?Upstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?ICD <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Downstream <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?Data1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?carries <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;}{?System1 ?Upstream ?ICD ;}{?ICD ?Downstream ?System2 ;} {?ICD ?carries ?Data1;}{?carries ?contains2 ?prop2} {?contains2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Upstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;} {?Downstream2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consume>;}{?System3 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}  {?ICD2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/InterfaceControlDocument> ;}{?Data2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;} {?carries2 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Payload>;} {?System3 ?Upstream2 ?ICD2 ;}{?ICD2 ?Downstream2 ?System1 ;} {?ICD2 ?carries2 ?Data2;} {?carries2 ?contains1 ?prop} {?contains1 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains> }} UNION {{?Provide <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?System1 ?Provide ?BLU}}}";
		SPARQLParse parse = new SPARQLParse();
		parse.createRepository();
		parse.parseIt(thisquery);
		parse.executeQuery(thisquery, engine);
		parse.loadBaseDB(engine.getProperty(Constants.OWL));
		this.rc = parse.rc;
		*/
		
		// this is where the block goes
		//figure out if we need to index jena for search and process for SUDOWL
		
		sudowl = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.GPSSudowl));
		prop = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.GPSProp));
		search = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.GPSSearch));
		try {
			boolean isError = false;
			if(rc != null && (extend || overlay))
			{
				logger.info("Creating the new model");
				Repository myRepository2 = new SailRepository(
			            new ForwardChainingRDFSInferencer(
			            new MemoryStore()));
				myRepository2.initialize();
				
				curRC = myRepository2.getConnection();
				curModel = ModelFactory.createDefaultModel();
			}
			StringBuffer subjects = new StringBuffer("");
			StringBuffer predicates = new StringBuffer("");
			StringBuffer objects = new StringBuffer("");
			if(!sjw.hasNext())
			{
				logger.info("Came into not having ANY data"); 
				return;
			}
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
				//predData.addConceptAvailable(st.getSubject());//, st.getSubject());
				//predData.addPredicateAvailable(st.getPredicate());//, st.getPredicate());

				if(subjects.indexOf("(<" + st.getSubject() + ">)") < 0)
				{
					if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
						subjects.append("(<").append(st.getSubject()).append(">)");
					else
						subjects.append("<").append(st.getSubject()).append(">");
				}
				if(predicates.indexOf("(<" + st.getPredicate() +">)") < 0)
				{
					if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
						predicates.append("(<").append(st.getPredicate()).append(">)");
					else
						predicates.append("<").append(st.getPredicate()).append(">");
				}
				// need to find a way to do this for jena too
				if(obj instanceof URI && !(obj instanceof com.hp.hpl.jena.rdf.model.Literal))
				{			
					if(objects.indexOf("(<" + obj +">)") < 0)
					{
						if(engine.getEngineType() == IEngine.ENGINE_TYPE.SESAME)
							objects.append("(<" + obj +">)");
						else
							objects.append("<" + obj +">");
					}
				}
				//addToJenaModel(st);
				addToSesame(st, false, false);
				if (search) addToJenaModel3(st);
			}			
			logger.debug("Subjects >>> " + subjects);
			logger.debug("Predicatss >>>> " + predicates);
			
			// now add the base relationships to the metamodel
			// this links the hierarchy that tool needs to the metamodel being queried
			// eventually this could be a SPIN
			// need to get the engine name and jam it - Done Baby
			if(!loadedOWLS.containsKey(engine.getEngineName()) && engine instanceof AbstractEngine)
			{
				if(this.baseRelEngine == null){
					this.baseRelEngine = ((AbstractEngine)engine).getBaseDataEngine();
				}
				else {
					RDFEngineHelper.addAllData(((AbstractEngine)engine).getBaseDataEngine(), this.baseRelEngine.getRC());
				}

				this.baseFilterHash.putAll(((AbstractEngine)engine).getBaseHash());
				
				RDFEngineHelper.addAllData(baseRelEngine, rc);
				loadedOWLS.put(engine.getEngineName(), engine.getEngineName());
			}
			logger.info("BaseQuery");
			// load the concept linkages
			// the concept linkages are a combination of the base relationships and what is on the file
			boolean loadHierarchy = !(subjects.equals("") && predicates.equals("") && objects.equals("")); 
			if(loadHierarchy)
			{
				try
				{
					RDFEngineHelper.loadConceptHierarchy(engine, subjects.toString(), objects.toString(), this);
					logger.debug("Loaded Concept");
					RDFEngineHelper.loadRelationHierarchy(engine, predicates.toString(), this);
					logger.debug("Loaded Relation");
				}catch(Exception ex)
				{
					ex.printStackTrace();
				}
			}
			String containsRelation = findContainsRelation();
			if(containsRelation == null)
				containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";

			if(sudowl){
				logger.info("Starting to load OWL");
				GraphOWLHelper.loadConceptHierarchy(rc, subjects.toString(), objects.toString(), this);
				GraphOWLHelper.loadRelationHierarchy(rc, predicates.toString(), this);
				GraphOWLHelper.loadPropertyHierarchy(rc,predicates.toString(), containsRelation, this);
				logger.info("Finished loading OWL");
			}
			if(true){
				logger.info("Starting to load properties");
				logger.info("Creating the properties");
				if(containsRelation != null)
				{
					// load local property hierarchy
					try
					{
						//loadPropertyHierarchy(predicates, containsRelation);
						RDFEngineHelper.loadPropertyHierarchy(engine,predicates.toString(), containsRelation, this);
						// now that this is done, we can query for concepts						
						//genPropertiesRemote(propertyQuery + "BINDINGS ?Subject { " + subjects + " " + predicates + " " + objects+ " } ");
						RDFEngineHelper.genPropertiesRemote(engine, subjects.toString(), objects.toString(), predicates.toString(), containsRelation, this);
						RDFEngineHelper.genNodePropertiesLocal(rc, containsRelation, this);
						RDFEngineHelper.genEdgePropertiesLocal(rc, containsRelation, this);
						logger.info("Loaded Properties");
					}catch(Exception ex)
					{
						ex.printStackTrace();
					}
					//genProperties(propertyQuery + predicates + " } ");
				}
			}

		} catch (RepositoryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}

	@Override
	public Object getData() {
		// TODO Auto-generated method stub
		return rc;
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
		
	}
}
