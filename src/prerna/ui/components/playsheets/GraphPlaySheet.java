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
package prerna.ui.components.playsheets;

import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.beans.PropertyVetoException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.JComponent;
import javax.swing.JInternalFrame;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.graph.SimpleGraph;

import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DelegateForest;
import edu.uci.ics.jung.visualization.GraphZoomScrollPane;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.picking.PickedState;
import edu.uci.ics.jung.visualization.renderers.BasicRenderer;
import edu.uci.ics.jung.visualization.renderers.Renderer;
import prerna.ds.TinkerFrame;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.ui.components.ControlData;
import prerna.ui.components.ControlPanel;
import prerna.ui.components.LegendPanel2;
import prerna.ui.components.NewHoriScrollBarUI;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.PropertySpecData;
import prerna.ui.components.VertexColorShapeData;
import prerna.ui.components.VertexFilterData;
import prerna.ui.components.playsheets.datamakers.IDataMaker;
import prerna.ui.components.specific.tap.DataLatencyPlayPopup;
import prerna.ui.helpers.NetworkGraphHelper;
import prerna.ui.main.listener.impl.GraphNodeListener;
import prerna.ui.main.listener.impl.GraphPlaySheetListener;
import prerna.ui.main.listener.impl.PickedStateListener;
import prerna.ui.main.listener.impl.PlaySheetColorShapeListener;
import prerna.ui.main.listener.impl.PlaySheetControlListener;
import prerna.ui.main.listener.impl.PlaySheetOWLListener;
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

/**
 * This class is responsible for managing all of the network graph items that the thick client is dependent on
 * These items consist mainly of the DelegateForest, VisualizationViewer, and surrounding panels the thick client uses
 * 
 * What this class does NOT do is manage the underlying data structure associated with the graph (the DataFrame)
 * Because this playsheet can have two very different types of DataFrames, the managing of these sit in the helpers
 * A GraphDataModel DataFrame will be managed by GraphGDMPlaySheetHelper
 * A TinkerFrame DataFrame will be managed by GraphTinkerPlaySheetHelper
 * 
 * The only way these helpers should be accessed is through this playsheet
 *
 */
public class GraphPlaySheet extends AbstractPlaySheet {

	private static final Logger logger = LogManager.getLogger(GraphPlaySheet.class.getName());
	public DelegateForest forest = null;
	public VisualizationViewer <SEMOSSVertex, SEMOSSEdge> view = null;
	protected String layoutName = Constants.FR;
	Layout layout2Use = null;
	public LegendPanel2 legendPanel = null;
	public JPanel cheaterPanel = new JPanel();
	public JTabbedPane jTab = new JTabbedPane();
	public JInternalFrame dataLatencyPopUp = null;
	public DataLatencyPlayPopup dataLatencyPlayPopUp = null;
	public ControlData controlData = new ControlData();
	public PropertySpecData predData = new PropertySpecData();
	protected SimpleGraph <SEMOSSVertex, SEMOSSEdge> graph = new SimpleGraph<SEMOSSVertex, SEMOSSEdge>(SEMOSSEdge.class);
	private NetworkGraphHelper helper;
	protected IDataMaker dataFrame;

	public VertexColorShapeData colorShapeData = new VertexColorShapeData();
	public VertexFilterData filterData = new VertexFilterData();
	
//	boolean sudowl, search, prop;
	
	//So that it doesn't get reset on extend and overlay etc. it must be stored
	VertexLabelFontTransformer vlft;
	EdgeLabelFontTransformer elft;
	VertexShapeTransformer vsht;
	
	protected Boolean ENABLE_SEARCH = Boolean.parseBoolean(DIHelper.getInstance().getProperty(Constants.GPSSearch));

	public ControlPanel searchPanel;
	
	public JSplitPane graphSplitPane;

	/**
	 * Constructor for GraphPlaySheet.
	 */
	public GraphPlaySheet()
	{
		logger.info("Graph PlaySheet ");
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
			//logger.debug("model size: " +rc.size());
		} catch (RuntimeException e) {
			// TODO: Specify exception
			logger.error(Constants.STACKTRACE, e);
		} catch (PropertyVetoException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
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
			logger.error(Constants.STACKTRACE, e);
		}
		
	}

	/**
	 * Method addInitialPanel.
	 */
	public void addInitialPanel()
	{
		setWindow();
		// create the listener and add the frame
		// if there is a view remove it
		// get
		GraphPlaySheetListener gpListener = new GraphPlaySheetListener();
		PlaySheetControlListener gpControlListener = new PlaySheetControlListener();
		PlaySheetOWLListener gpOWLListener = new PlaySheetOWLListener();
		PlaySheetColorShapeListener gpColorShapeListener = new PlaySheetColorShapeListener();
		
		this.addInternalFrameListener(gpListener);
		this.addInternalFrameListener(gpControlListener);
		this.addInternalFrameListener(gpOWLListener);
		this.addInternalFrameListener(gpColorShapeListener);

		
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
			if (ENABLE_SEARCH)
			{
//				searchPanel.searchCon.indexStatements(dataFrame);
			}
			GraphZoomScrollPane gzPane = new GraphZoomScrollPane(view);
			gzPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
			gzPane.getHorizontalScrollBar().setUI(new NewHoriScrollBarUI());
			graphSplitPane.setLeftComponent(searchPanel);
			
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
		}catch(RuntimeException ex)
		{
			logger.debug(ex);
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
		} catch (NoSuchMethodException e) {
			fail++;
			logger.info(e);
		} catch (InstantiationException e) {
			fail++;
			logger.info(e);
		} catch (IllegalAccessException e) {
			fail++;
			logger.info(e);
		} catch (IllegalArgumentException e) {
			fail++;
			logger.info(e);
		} catch (InvocationTargetException e) {
			fail++;
			logger.info(e);
		}
		try{
			constructor = layoutClass.getConstructor(edu.uci.ics.jung.graph.Graph.class);
			layout2Use  = (Layout)constructor.newInstance(forest);
		} catch (NoSuchMethodException e) {
			fail++;
			logger.info(e);
		} catch (InstantiationException e) {
			fail++;
			logger.info(e);
		} catch (IllegalAccessException e) {
			fail++;
			logger.info(e);
		} catch (IllegalArgumentException e) {
			fail++;
			logger.info(e);
		} catch (InvocationTargetException e) {
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
	 * Method getForest.
	 * @return DelegateForest
	 */
	public DelegateForest getForest() {
		forest = new DelegateForest();
//		semossGraph.graph = new SimpleGraph<SEMOSSVertex, SEMOSSEdge>(SEMOSSEdge.class);
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
	 * Method getView.
	 * @return VisualizationViewer
	 */
	public VisualizationViewer getView()
	{
		return view;
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
//			searchVertices.putAll(searchPanel.searchCon.cleanResHash);
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

	/**
	 * Method genAllData.
	 */
	public void genAllData()
	{
		filterData.fillRows();
		filterData.fillEdgeRows();
		controlData.generateAllRows();
		colorShapeData.setTypeHash(filterData.typeHash);
		colorShapeData.setCount(filterData.count);
		colorShapeData.fillRows();
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
		
	}
	
	protected void processControlData(SEMOSSEdge edge){
		String edgeType = edge.getProperty(Constants.EDGE_TYPE).toString();
		for(String prop : edge.getPropertyKeys()){
			controlData.addProperty(edgeType, prop);
		}
	}
	
	protected void processControlData(SEMOSSVertex vert){
		String vertType = vert.getProperty(Constants.VERTEX_TYPE).toString();
		for(String prop : vert.getPropertyKeys()){
			controlData.addProperty(vertType, prop);
		}
	}

	@Override
	public void createData() {
		
	}

	@Override
	public Hashtable<String, String> getDataTableAlign() {
		// Need to figure out how graph will align to table....
		return null;
	}

	@Override
	public void setQuery(String query) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void processQueryData() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IDataMaker getDefaultDataMaker() {
		return new TinkerFrame();
	}
	
	/**
	 * Method getGraph.
	 * @return Graph
	 */
	public Graph getGraph()
	{
		return graph;
	}
	
	@Override
	public void refineView() {
		this.helper.refineView();
	}
	
	@Override
	public void overlayView() {
		this.helper.overlayView();
	}
	
	@Override
	public void setDataMaker(IDataMaker data) {
		this.dataFrame = data;
		if(this.dataFrame instanceof TinkerFrame){
			this.helper = new GraphTinkerPlaySheetHelper(this);
		}
		else{
			this.helper = new GraphGDMPlaySheetHelper(this);
		}
		
	}
	
	@Override
	public IDataMaker getDataMaker() {
		return this.dataFrame;
	}

	public void removeView(){
		helper.removeView();
	}
	public void redoView(){
		helper.redoView();
	}
	public void undoView(){
		helper.undoView();
	}
	
	public boolean getSudowl(){
		return helper.getSudowl();
	}
	
	public Collection<SEMOSSVertex> getVerts(){
		return helper.getVerts();
	}
	
	public Collection<SEMOSSEdge> getEdges(){
		return helper.getEdges();
	}

	/**
	 * Method getPredicateData.
	 * @return PropertySpecData
	 */
	public PropertySpecData getPredicateData() {
		return predData;
	}
	
	public String addNewConcepts(String sub, String pred, String obj){
		return helper.addNewConcepts(sub, pred, obj);
	}
	
	public void removeExistingConcepts(Vector <String> subVector){
		helper.removeExistingConcepts(subVector);
	}
	
	public void clearStores(){
		helper.clearStores();
	}
	
	public void exportDB(){
		helper.exportDB();
	}
	
	public void createForest(){
		helper.createForest();
	}
	
	@Override
	public void createView(){
		helper.createView();
	}
	
	public Hashtable<String, SEMOSSVertex> getVertStore(){
		return helper.getVertStore();
	}
	
	public Hashtable<String, SEMOSSEdge> getEdgeStore(){
		return helper.getEdgeStore();
	}
}
