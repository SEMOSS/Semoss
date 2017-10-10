//package prerna.ui.components.playsheets;
//
//import java.awt.Dimension;
//import java.awt.GridBagConstraints;
//import java.util.HashMap;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Map;
//
//import javax.swing.JComponent;
//import javax.swing.JPanel;
//import javax.swing.JProgressBar;
//import javax.swing.JScrollPane;
//import javax.swing.JTabbedPane;
//import javax.swing.JTable;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
//import prerna.om.SEMOSSParam;
//import prerna.ui.components.GridScrollPane;
//import prerna.ui.components.NewScrollBarUI;
//import prerna.util.Constants;
//import prerna.util.DIHelper;
//
//public class WekaAprioriVizPlaySheet extends BrowserPlaySheet{
//
//	private static final Logger LOGGER = LogManager.getLogger(WekaAprioriPlaySheet.class.getName());
//
//	private WekaAprioriAlgorithm alg;
//
//	private int numRules = 10; // number of rules to output
//	private double confPer = 0.9; // min confidence lvl (percentage)
//	private double minSupport = 0.1; // min number of rows required for rule (percentage of total rows of data)
//	private double maxSupport = 1.0; // max number of rows required for rule (percentage of total rows of data)	
//	private List<String> skipAttributes;
//	
//	public WekaAprioriVizPlaySheet() {
//		super();
//		this.setPreferredSize(new Dimension(800,600));
//		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
//		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singleaxisbubbleassociation.html";
//	}
//	
//	@Override
//	public void createData() {
//		if(dataFrame == null || dataFrame.isEmpty())
//			super.createData();
//	}
//
//	@Override
//	public void processQueryData() {
//		this.dataHash = alg.generateDecisionRuleVizualization();
//	}
//	
//	@Override
//	public void runAnalytics() {
//		alg = new WekaAprioriAlgorithm();
//		List<SEMOSSParam> options = alg.getOptions();
//		Map<String, Object> selectedOptions = new HashMap<String, Object>();
//		selectedOptions.put(options.get(0).getName(), numRules);
//		selectedOptions.put(options.get(1).getName(), confPer);
//		selectedOptions.put(options.get(2).getName(), minSupport);
//		selectedOptions.put(options.get(3).getName(), maxSupport);
//		selectedOptions.put(options.get(4).getName(), skipAttributes);
//		alg.setSelectedOptions(selectedOptions);
//		dataFrame.performAnalyticAction(alg);
//		
//		alg.generateDecisionRuleTable();
//	}
//	
//	@Override
//	public Hashtable getDataMakerOutput(String... selectors) {
//		//TODO: remove this from getData() to call the super method
//		dataHash.put("id", this.questionNum==null? "": this.questionNum);
//		String className = "";
//		Class<?> enclosingClass = getClass().getEnclosingClass();
//		if (enclosingClass != null) {
//			className = enclosingClass.getName();
//		} else {
//			className = getClass().getName();
//		}
//		dataHash.put("layout", className);
//		dataHash.put("title", this.title==null? "": this.title);
//		
//		Hashtable<String, String> specificData = new Hashtable<String, String>();
//		specificData.put("x-axis", "Confidence");
//		specificData.put("z-axis", "Count");
//		dataHash.put("specificData", specificData);
//		return dataHash;
//	}
//	
//	@Override
//	public List<Object[]> getList() {
//		return alg.getTabularData();
//	}
//	
//	@Override
//	public String[] getNames() {
//		return alg.getColumnHeaders();
//	}
//	
//	public int getNumRules() {
//		return numRules;
//	}
//
//	public void setNumRules(int numRules) {
//		this.numRules = numRules;
//	}
//
//	public double getConfPer() {
//		return confPer;
//	}
//
//	public void setConfPer(double confPer) {
//		this.confPer = confPer;
//	}
//
//	public double getMinSupport() {
//		return minSupport;
//	}
//
//	public void setMinSupport(double minSupport) {
//		this.minSupport = minSupport;
//	}
//
//	public double getMaxSupport() {
//		return maxSupport;
//	}
//
//	public void setMaxSupport(double maxSupport) {
//		this.maxSupport = maxSupport;
//	}
//	
//	public void setSkipAttributes(List<String> skipAttributes) {
//		this.skipAttributes = skipAttributes;
//	}
//
//	/////////////////////////////SWING DEPENDENT CODE/////////////////////////////
//	@Override
//	public void addPanel() {
//		if (jTab == null) {
//			super.addPanel();
//		} else {
//			String lastTabName = jTab.getTitleAt(jTab.getTabCount() - 1);
//			LOGGER.info("Parsing integer out of last tab name");
//			int count = 1;
//			if (jTab.getTabCount() > 1)
//				count = Integer.parseInt(lastTabName.substring(0, lastTabName.indexOf("."))) + 1;
//			addPanelAsTab(count + ". Apriori Data");
//			addGridTab(count + ". Apriori Raw Data");
//		}
//	}
//
//	public void addGridTab(String tabName) {
//		table = new JTable();
//		GridScrollPane gsp = null;
//		gsp = new GridScrollPane(alg.getColumnHeaders(), alg.getTabularData());
//		gsp.addHorizontalScroll();
//		jTab.addTab(tabName, gsp);
//	}
//
//	public void addScrollPanel(JPanel panel, JComponent obj) {
//		JScrollPane scrollPane = new JScrollPane(obj);
//		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
//		scrollPane.setAutoscrolls(true);
//
//		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
//		gbc_scrollPane.fill = GridBagConstraints.BOTH;
//		gbc_scrollPane.gridx = 0;
//		gbc_scrollPane.gridy = 0;
//		panel.add(scrollPane, gbc_scrollPane);
//	}
//
//	public void setJTab(JTabbedPane jTab) {
//		this.jTab = jTab;
//	}
//
//	public void setJBar(JProgressBar jBar) {
//		this.jBar = jBar;
//	}
//}
