package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
import prerna.util.CSSApplication;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class WekaAprioriVizPlaySheet extends BrowserPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(WekaAprioriPlaySheet.class.getName());

	private WekaAprioriAlgorithm alg;

	private int numRules = 100; // number of rules to output
	private double confPer = -1; // min confidence lvl (percentage)
	private double minSupport = -1; // min number of rows required for rule (percentage of total rows of data)
	private double maxSupport = -1; // max number of rows required for rule (percentage of total rows of data)
	
	public WekaAprioriVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singleaxisbubbleassociation.html";
	}
	
	@Override
	public void createData() {
		if (list == null || list.isEmpty())
			super.createData();
		else {
			dataHash = processQueryData();
		}
	}

	@Override
	public Hashtable processQueryData() {
		runAlgorithm();
		return alg.generateDecisionRuleVizualization();
	}
	
	@Override
	public Object getData() {
		//TODO: remove this from getData() to call the super method
		dataHash.put("id", this.questionNum==null? "": this.questionNum);
		String className = "";
		Class<?> enclosingClass = getClass().getEnclosingClass();
		if (enclosingClass != null) {
			className = enclosingClass.getName();
		} else {
			className = getClass().getName();
		}
		dataHash.put("playsheet", className);
		dataHash.put("title", this.title==null? "": this.title);
		
		Hashtable<String, String> specificData = new Hashtable<String, String>();
		specificData.put("x-axis", "Confidence");
		specificData.put("z-axis", "Count");
		dataHash.put("specificData", specificData);
		return dataHash;
	}
	
	public void runAlgorithm() {
		LOGGER.info("Creating apriori algorithm for instance: " + names[0]);
		alg = new WekaAprioriAlgorithm(list, names);
		if(numRules > 0) {
			alg.setNumRules(numRules);
		}
		if(confPer >= 0 && confPer <= 1.0) {
			alg.setConfPer(confPer);
		}
		if(minSupport >= 0 && minSupport < 1) {
			alg.setMinSupport(minSupport);
		}
		if(maxSupport > 0 && maxSupport <=1) {
			alg.setMaxSupport(maxSupport);
		}
		try {
			alg.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Method addPanel. Creates a panel and adds the table to the panel.
	 */
	@Override
	public void addPanel()
	{
		//if this is to be a separate playsheet, create the tab in a new window
		//otherwise, if this is to be just a new tab in an existing playsheet,
		if(jTab==null) {
			super.addPanel();
		} else {
			String lastTabName = jTab.getTitleAt(jTab.getTabCount()-1);
			LOGGER.info("Parsing integer out of last tab name");
			int count = 1;
			if(jTab.getTabCount()>1)
				count = Integer.parseInt(lastTabName.substring(0,lastTabName.indexOf(".")))+1;
			addPanelAsTab(count+". Association Learning");
		}
		new CSSApplication(getContentPane());
	}
	
	public int getNumRules() {
		return numRules;
	}

	public void setNumRules(int numRules) {
		this.numRules = numRules;
	}

	public double getConfPer() {
		return confPer;
	}

	public void setConfPer(double confPer) {
		this.confPer = confPer;
	}

	public double getMinSupport() {
		return minSupport;
	}
	
	public void setMinSupport(double minSupport) {
		this.minSupport = minSupport;
	}

	public double getMaxSupport() {
		return maxSupport;
	}

	public void setMaxSupport(double maxSupport) {
		this.maxSupport = maxSupport;
	}
}
