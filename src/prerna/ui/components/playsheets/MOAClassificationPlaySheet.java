//package prerna.ui.components.playsheets;
//
//import java.awt.GridBagConstraints;
//import java.text.DecimalFormat;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.Hashtable;
//import java.util.List;
//import java.util.Map;
//
//import javax.swing.JComponent;
//import javax.swing.JPanel;
//import javax.swing.JProgressBar;
//import javax.swing.JScrollPane;
//import javax.swing.JTabbedPane;
//
//import org.apache.log4j.LogManager;
//import org.apache.log4j.Logger;
//
//import prerna.algorithm.learning.moa.HoeffdingTreeAlgorithm;
//import prerna.om.SEMOSSParam;
//import prerna.ui.components.NewScrollBarUI;
//
//public class MOAClassificationPlaySheet extends DendrogramPlaySheet {
//
//	private static final Logger LOGGER = LogManager.getLogger(MOAClassificationPlaySheet.class.getName());
//	
//	private String modelName;
//	private HoeffdingTreeAlgorithm alg = null;
//	private String className = "";
//	private double gracePeriod;
//	private double confValue;
//	private double tieThreshold;
//	private int classIndex = -1;
//	
//	private List<String> skipAttributes;
//
//	public MOAClassificationPlaySheet() {
//		super();
//	}
//	
//	@Override
//	public void createData() {
//		if(dataFrame == null || dataFrame.isEmpty())
//			super.createData();
//	}
//	
//	@Override
//	public void runAnalytics() {
//		alg = new HoeffdingTreeAlgorithm();
//		try {
//			List<SEMOSSParam> options = alg.getOptions();
//			Map<String, Object> selectedOptions = new HashMap<String, Object>();
//			String[] headers = dataFrame.getColumnHeaders();
//			// get index of classifier
//			for (int i = 0; i < headers.length; i++) {
//				System.out.println(headers[i]+" "+this.className);
//				if (headers[i].equals(this.className)) {
//					classIndex = i;
//					System.out.println(i);
//				}
//			}
//			selectedOptions.put(options.get(0).getName(), classIndex);       // last index
//			selectedOptions.put(options.get(1).getName(), confValue);        // split confidence
//			selectedOptions.put(options.get(2).getName(), gracePeriod);      // grace pd
//			selectedOptions.put(options.get(3).getName(), tieThreshold);       // last index
//			selectedOptions.put(options.get(4).getName(), skipAttributes);   // skip attributes
//			alg.setSelectedOptions(selectedOptions);
//			dataFrame.performAnalyticAction(alg);
//		} catch(Exception e) {
//		 	classLogger.error(Constants.STACKTRACE, e);
//		}
//		//alg.processTreeString();
//	}
//	
//	@Override
//	public void processQueryData() {
//		Map<String, Map> rootMap = alg.processTreeString();
//		
//		System.out.println(rootMap);
//		HashSet hashSet = new HashSet();
//		
//		processTree(rootMap, hashSet);
//
//		String root = "Dataset";
//		
//		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
//		allHash.put("name", root);
//		allHash.put("children", hashSet);
//		
//		DecimalFormat df = new DecimalFormat("#%");
//		ArrayList<Hashtable<String, Object>> statList = new ArrayList<Hashtable<String, Object>>();
//		Hashtable<String, Object> statHash = new Hashtable<String, Object>();
//		statHash.put("Accuracy", df.format((double)(alg.getResultMetadata().get("accuracy"))/100));
//		statList.add(statHash);
//		allHash.put("stats", statList);
//		
//		this.dataHash = allHash ;
//	}
//	
//	public void setGracePeriod(int gracePeriod) {
//		this.gracePeriod = (double)gracePeriod/100;
//		System.out.println("gracePd: "+this.gracePeriod);
//	}
//	
//	public void setConfValue(int confValue) {
//		this.confValue = (double)confValue/100;
//		System.out.println("CV: "+this.confValue);
//	}
//	
//	public void setTieThreshold(int tieThreshold) {
//		this.tieThreshold = (double)tieThreshold/100;
//		System.out.println("CV: "+this.tieThreshold);
//	}
//	
//	public void setModelName(String modelName) {
//		this.modelName = modelName;
//	}
//	
//	public void setClassColumn(String className){
//		System.out.println("SET: "+className);
//		this.className = className;
//	}
//	
//	public String getClassColumn() {
//		return className;
//	}
//	
//	public void setSkipAttributes(List<String> skipColumns) {
//		this.skipAttributes = skipColumns;
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
//			addPanelAsTab(count + ". Classification");
//		}
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
//	
//	
//	
//}
