package prerna.ui.components.specific.cbp;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.JButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import com.bigdata.rdf.model.BigdataLiteralImpl;

import prerna.om.GraphDataModel;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.util.StatementCollector;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.impl.ColumnChartGroupedStackedListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GBCColumnChartPlaySheet extends BrowserPlaySheet{

	private static final Logger logger = LogManager.getLogger(GBCColumnChartPlaySheet.class.getName());
	GraphDataModel gdm = new GraphDataModel();
	Hashtable<String, String> taxonomyHash = new Hashtable<String, String>();
	Hashtable<String, String> topTaxonomyHash;
	String taxonomyQuery = "";
	Hashtable<String, ArrayList<Hashtable<String,Object>>> seriesHash = new Hashtable<String, ArrayList<Hashtable<String,Object>>>();
	
	public GBCColumnChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/columnchart.html";
	}
	
	@Override
	public void createControlPanel(){
		controlPanel = new ChartControlPanel();
		controlPanel.addExportButton(1);
		
		ChartControlPanel ctrlPanel = this.getControlPanel();
		GridBagConstraints gbc_btnGroupedStacked = new GridBagConstraints();
		gbc_btnGroupedStacked.insets = new Insets(10, 0, 0, 5);
		gbc_btnGroupedStacked.anchor = GridBagConstraints.EAST;
		gbc_btnGroupedStacked.gridx = 0;
		gbc_btnGroupedStacked.gridy = 0;
		JButton transitionGroupedStacked = new JButton("Transition Grouped");
		ColumnChartGroupedStackedListener gsListener = new ColumnChartGroupedStackedListener();
		gsListener.setBrowser(this.browser);
		transitionGroupedStacked.addActionListener(gsListener);
		ctrlPanel.add(transitionGroupedStacked, gbc_btnGroupedStacked);

		this.controlPanel.setPlaySheet(this);
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		fillTaxonomyHash();

		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			
			Double clientY = ((BigdataLiteralImpl)elemValues[1]).doubleValue();
			Double groupY = ((BigdataLiteralImpl)elemValues[3]).doubleValue();
			String seriesName = taxonomyHash.get(elemValues[4]+"");
			if(seriesHash.containsKey(seriesName)){
				ArrayList<Hashtable<String,Object>> seriesArray = seriesHash.get(seriesName);
				
				Hashtable<String, Object> clientElementHash = seriesArray.get(0);
				clientY = clientY + (Double) clientElementHash.get("y");
				clientElementHash.put("y", clientY);
				
				Hashtable<String, Object> groupElementHash = seriesArray.get(1);
				groupY = groupY + (Double) groupElementHash.get("y");
				groupElementHash.put("y", groupY);
			}
			else{
				ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
				storeNewValue(elemValues[0].toString(), clientY, seriesName, seriesArray, 0); //store the new client value
				storeNewValue(elemValues[2].toString(), groupY, seriesName, seriesArray, 1); //store the new group value
			}
			
		}
		ArrayList< ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList< ArrayList<Hashtable<String, Object>>>();
		dataObj.addAll(seriesHash.values());
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("dataSeries", dataObj);
		
		return columnChartHash;
	}
	
	private void storeNewValue(String x, Double y, String seriesName, ArrayList<Hashtable<String,Object>> seriesArray, int position){
		seriesHash.put(seriesName, seriesArray);
		Hashtable<String, Object> elementHash = new Hashtable();
		elementHash.put("x", x);
		elementHash.put("y", y);
		elementHash.put("seriesName", seriesName);
		if(this.topTaxonomyHash!=null){
			elementHash.put("topLevel", this.topTaxonomyHash.get(seriesName));
		}
		seriesArray.add(position, elementHash);
	}
	
	private void fillTaxonomyHash(){

		logger.info("Begining fill taxonomy hash with q: " + this.taxonomyQuery);

		// Run the time query
		SesameJenaSelectWrapper taxWrapper = new SesameJenaSelectWrapper();
		taxWrapper.setQuery(this.taxonomyQuery);
		taxWrapper.setEngine(this.engine);
		try{
			taxWrapper.executeQuery();	
		}
		catch (RuntimeException e)
		{
			e.printStackTrace();
		}		

		// get the bindings from it
		String[] taxNames = taxWrapper.getVariables();
		if(taxNames.length==3){
			topTaxonomyHash = new Hashtable<String, String>();
		}
		
		// as we process the rows, add to hash
		try {
			while(taxWrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = taxWrapper.next();
				this.taxonomyHash.put(sjss.getRawVar(taxNames[0]) + "", sjss.getRawVar(taxNames[1]) + "");
				if(taxNames.length == 3){//need to store top taxonomy info for grid
					this.topTaxonomyHash.put(sjss.getRawVar(taxNames[1]) + "", sjss.getRawVar(taxNames[2]) + "");
				}
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
		logger.info("Done with taxonomy hash");
	}

	@Override
	public void setQuery(String query) {
		String [] tokens = query.split("\\+\\+\\+");
		// format is mainQuery+++taxonomyQuery
		for (int queryIdx = 0; queryIdx < tokens.length; queryIdx++){
			String token = tokens[queryIdx];
			if(queryIdx == 0){
				this.query = token;
				logger.info("Set query 1 " + token);
			}
			else if (queryIdx == 1){
				this.taxonomyQuery = token;
				logger.info("Set query 2 " + token);
			}
		}
	}
	
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getRawVar(varName);
	}
	
}
