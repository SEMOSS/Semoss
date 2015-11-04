package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.unsupervised.som.SOMRoutine;
import prerna.algorithm.learning.unsupervised.som.SelfOrganizingMapGridViewer;
import prerna.om.SEMOSSParam;
import prerna.ui.components.GridScrollPane;
import prerna.ui.components.NewScrollBarUI;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class SelfOrganizingMap3DBarChartPlaySheet extends BrowserPlaySheet {

	private static final Logger LOGGER = LogManager.getLogger(SelfOrganizingMap3DBarChartPlaySheet.class.getName());

	private SOMRoutine alg;
	private String[] columnHeaders;
	private double[][] zAxisGrid;

	private int instanceIndex = 0;
	private double initalRadius = 2.0;
	private double learningRate = 0.07;
	private double tau = 7.5;
	private int maxIterations = 15;
	private int gridWidth;
	private int gridLength;
	private List<String> skipAttributes;
	
	public SelfOrganizingMap3DBarChartPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800, 600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/3dbarchart.html";
	}
	
	@Override
	public void createData() {
		if(dataFrame == null || dataFrame.isEmpty())
			super.createData();
	}

	@Override
	public void runAnalytics() {
		alg = new SOMRoutine();
		List<SEMOSSParam> options = alg.getOptions();
		Map<String, Object> selectedOptions = new HashMap<String, Object>();
		selectedOptions.put(options.get(0).getName(), instanceIndex); // default of 0 is acceptable
		selectedOptions.put(options.get(1).getName(), initalRadius);
		selectedOptions.put(options.get(2).getName(), learningRate);
		selectedOptions.put(options.get(3).getName(), tau);
		selectedOptions.put(options.get(4).getName(), maxIterations);
		if(gridWidth > 0) {
			selectedOptions.put(options.get(5).getName(), gridWidth);
		}
		if(gridLength > 0) {
			selectedOptions.put(options.get(6).getName(), gridLength);
		}
		selectedOptions.put(options.get(8).getName(), skipAttributes);
		alg.setSelectedOptions(selectedOptions);
		dataFrame.performAnalyticTransformation(alg);
		this.columnHeaders = dataFrame.getColumnHeaders();
		zAxisGrid = SelfOrganizingMapGridViewer.generateZAxisGridValues(alg.getGridLength(), alg.getGridWidth(), alg.getNumInstancesInGrid());
	}
	
	@Override 
	public void processQueryData() {
		Hashtable<String, Object> data = new Hashtable<String, Object>();
		data.put("specificData", zAxisGrid);
		data.put("data", dataFrame.getData());
		data.put("headers", dataFrame.getColumnHeaders());
		this.dataHash = data ;
	}
	
	public List<String> getChangedCol() {
		return alg.getChangedColumns();
	}
	
	@Override
	public Hashtable getDataMakerOutput() {
		//TODO: remove this from getData() to call the super method
		dataHash.put("id", this.questionNum==null? "": this.questionNum);
		String className = "";
		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null) {
			className = enclosingClass.getName();
		} else {
			className = getClass().getName();
		}
		dataHash.put("layout", className);
		dataHash.put("title", this.title==null? "": this.title);
		
		return dataHash;
	}
	
	public int getInstanceIndex() {
		return instanceIndex;
	}

	public void setInstanceIndex(int instanceIndex) {
		this.instanceIndex = instanceIndex;
	}

	public double getInitalRadius() {
		return initalRadius;
	}

	public void setInitalRadius(double initalRadius) {
		this.initalRadius = initalRadius;
	}

	public double getLearningRate() {
		return learningRate;
	}

	public void setLearningRate(double learningRate) {
		this.learningRate = learningRate;
	}

	public double getTau() {
		return tau;
	}

	public void setTau(double tau) {
		this.tau = tau;
	}

	public int getMaxIterations() {
		return maxIterations;
	}

	public void setMaxIterations(int maxIterations) {
		this.maxIterations = maxIterations;
	}

	public int getGridWidth() {
		return gridWidth;
	}

	public void setGridWidth(int gridWidth) {
		this.gridWidth = gridWidth;
	}

	public int getGridLength() {
		return this.gridLength;
	}

	public void setGridLength(int gridLength) {
		this.gridLength = gridLength;
	}
	
	public void setSkipAttributes(List<String> skipAttributes) {
		this.skipAttributes = skipAttributes;
	}
	
	/////////////////////////////SWING DEPENDENT CODE/////////////////////////////
	@Override
	public void addPanel() {
		if (jTab == null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if (jTab.getTabCount() > 1)
				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
			addPanelAsTab(count + ". SOM Viz Data");
			addGridTab(count + ". SOM Raw Data");
		}
	}

	public void addGridTab(String tabName) {
		table = new JTable();
		GridScrollPane gsp = null;
		gsp = new GridScrollPane(dataFrame.getColumnHeaders(), dataFrame.getData());
		gsp.addHorizontalScroll();
		jTab.addTab(tabName, gsp);
	}

	public void addScrollPanel(JPanel panel, JComponent obj) {
		JScrollPane scrollPane = new JScrollPane(obj);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);

		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		panel.add(scrollPane, gbc_scrollPane);
	}

	public void setJTab(JTabbedPane jTab) {
		this.jTab = jTab;
	}

	public void setJBar(JProgressBar jBar) {
		this.jBar = jBar;
	}

}