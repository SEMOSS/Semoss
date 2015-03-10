package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

import prerna.algorithm.learning.weka.WekaAprioriAlgorithm;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class WekaAprioriVizPlaySheet extends BrowserPlaySheet{

	private static final Logger LOGGER = LogManager.getLogger(WekaAprioriPlaySheet.class.getName());

	private WekaAprioriAlgorithm alg;

	private int numRules = 100; // number of rules to output
	private double confPer = -1; // min confidence lvl (percentage)
	private double minSupport = -1; // min number of rows required for rule (percentage of total rows of data)
	private double maxSupport = -1; // max number of rows required for rule (percentage of total rows of data)
	
	
	////////////////////////////////TODO: change heatmap in fileName
	public WekaAprioriVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singleaxisbubbleassociation.html";//TODO change to new name
	}
	
	@Override
	public void createData() {
		generateData();
		runAlgorithm();
		dataHash = processQueryData();
		
		Gson gson = new Gson();
		System.out.println(gson.toJson(dataHash));
	}

	@Override
	public Hashtable processQueryData() {
		return alg.generateDecisionRuleVizualization();
	}
	
	private void generateData() {
		if(query!=null) {
			list = new ArrayList<Object[]>();

			ISelectWrapper sjsw = Utility.processQuery(engine, query);
			names = sjsw.getVariables();
			int length = names.length;
			while(sjsw.hasNext()) {
				ISelectStatement sjss = sjsw.next();
				Object[] row = new Object[length];
				int i = 0;
				for(; i < length; i++) {
					row[i] = sjss.getVar(names[i]);
				}
				list.add(row);
			}
		}
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
