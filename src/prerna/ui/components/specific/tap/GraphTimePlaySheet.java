package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.awt.Toolkit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import prerna.om.GraphDataModel;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GraphTimePlaySheet extends BrowserPlaySheet{

	private static final Logger logger = LogManager.getLogger(GraphTimePlaySheet.class.getName());
	protected GraphDataModel gdm = new GraphDataModel();
	ArrayList<String[]> graphQueryArray = new ArrayList<String[]>(); // [0] = engine; [1] = query
	ArrayList<String[]> timeQueryArray = new ArrayList<String[]>(); // [0] = engine; [1] = query
	
	/**
	 * Constructor for GraphTimePlaySheet.
	 */
	public GraphTimePlaySheet() {
		super();
		this.setPreferredSize(Toolkit.getDefaultToolkit().getScreenSize());
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/network-timeline.html";
	}
	
	@Override
	public void createView(){
		this.dataHash = (Hashtable) this.getData();
		super.createView();
	}
	
	@Override
	public void createData()
	{
		gdm.setPropSudowlSearch(false, false, false);
		for (int qIdx = 0; qIdx < this.graphQueryArray.size(); qIdx ++ ){
			String[] queryArray = this.graphQueryArray.get(qIdx);
			gdm.processData(queryArray[0], (IEngine) DIHelper.getInstance().getLocalProp(queryArray[1]));
		}

		logger.info("Creating the base Graph");
//		gdm.fillStoresFromModel();
		gdm.genBaseGraph();
	}

	@Override
	public void setQuery(String query) {
		query = query.trim();
		if(query.contains("$")){
			StringTokenizer queryTokens = new StringTokenizer(query, "$");
			int graphCount = 0;
			int timeCount = 0;
			boolean graph = false;
			// format is query1$engine1$query2$engine2 etc.
			for (int queryIdx = 0; queryTokens.hasMoreTokens(); queryIdx++){
				String token = queryTokens.nextToken();
				if (queryIdx % 2 == 0){
					if (token.startsWith("CONSTRUCT")){
						this.graphQueryArray.add(new String[]{token, null});
						graph = true;
					}
					else {
						this.timeQueryArray.add(new String[]{token, null});
					}
				}
				else {
					if(graph == true){
						StringTokenizer dbTokens = new StringTokenizer(token, ",");
						String thisQuery = this.graphQueryArray.get(graphCount)[0];
						this.graphQueryArray.get(graphCount)[1] = dbTokens.nextToken();
						graphCount++;
						while (dbTokens.hasMoreTokens()){
							this.graphQueryArray.add(new String[]{thisQuery, dbTokens.nextToken()});
							graphCount++;
						}
						graph = false;
					}
					else{
						this.timeQueryArray.get(timeCount)[1] = token;
						timeCount++;
					}
				}
			}
		}
	}
	
	@Override
	public Object getData() {
		Hashtable returnHash = (Hashtable) super.getData();
		returnHash.put("nodes", gdm.getVertStore());
		returnHash.put("edges", gdm.getEdgeStore().values());
		return returnHash;
	}
	
	@Override
	public void runAnalytics()
	{
		for (int qIdx = 0; qIdx < this.timeQueryArray.size(); qIdx ++){
			String[] queryArray = this.timeQueryArray.get(qIdx);
			processTimeData(queryArray[0], (IEngine) DIHelper.getInstance().getLocalProp(queryArray[1]));
		}
		Hashtable retHash = (Hashtable) this.getData();
		Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
		
		System.err.println(gson.toJson(retHash));
	}
	
	/**
	 * Run the time query
	 * First column must be edge or node
	 * Second column must be unique name for phase (e.g. Requirements)
	 * Rest of the columns are time properties to be associated with that edge or node
	 * Go through query, getting edge or node and attaching time info
	 */
	protected void processTimeData(String timeQuery, IEngine timeEngine)
	{
		logger.info("Begining processTimeData with q: " + timeQuery + " on engine " + timeEngine);

		// Run the time query
		SesameJenaSelectWrapper timeWrapper = new SesameJenaSelectWrapper();
		if(timeEngine!= null){

			timeWrapper.setQuery(timeQuery);
			timeWrapper.setEngine(timeEngine);
			try{
				timeWrapper.executeQuery();	
			}
			catch (RuntimeException e)
			{
				e.printStackTrace();
			}		

			// get the bindings from it
			String[] timeNames = timeWrapper.getVariables();
			int count = 0;
			
			Hashtable<String, SEMOSSVertex> vertStore = gdm.getVertStore();
			Hashtable<String, SEMOSSEdge> edgeStore = gdm.getEdgeStore();
			// as we process the rows, add info to node/edge
			try {
				while(timeWrapper.hasNext())
				{
					SesameJenaSelectStatement sjss = timeWrapper.next();
					
					//column 0 must be edge/node, so lets get that first... if it doesn't exist, skip the row
					String uri = sjss.getRawVar(timeNames[0]).toString();
					SEMOSSEdge edge = edgeStore.get(uri);
					SEMOSSVertex vert = vertStore.get(uri);
					
					if(vert != null || edge != null){
						String phaseKey = sjss.getVar(timeNames[1]).toString();
						Hashtable<String, Hashtable<String, Object>> timeHash = new Hashtable<String, Hashtable<String, Object>>();
						Hashtable<String, Object> phaseHash = new Hashtable<String, Object>();
						for(int colIndex = 1;colIndex < timeNames.length;colIndex++)
						{
							phaseHash.put(timeNames[colIndex], sjss.getVar(timeNames[colIndex]));
						}
						timeHash.put(phaseKey, phaseHash);
						storeTimeHash(edge, vert, timeHash);
					}
				}
			} catch (RuntimeException e) {
				logger.fatal(e);
			}

		}
		logger.info("Done with forest creation");
									
	}

	public void storeTimeHash(SEMOSSEdge edge, SEMOSSVertex vert, Hashtable timeHash){
		//either edge or vert will be null
		if(edge!=null){
			//store time hash on edge
			Hashtable fullTimeHash = (Hashtable) edge.getProperty("timeHash");
			if(fullTimeHash == null)
				fullTimeHash = new Hashtable();
			fullTimeHash.putAll(timeHash);
			edge.setProperty("timeHash", fullTimeHash);
		}
		if(vert!=null){
			//store time hash on vert
			Hashtable fullTimeHash = (Hashtable) vert.getProperty("timeHash");
			if(fullTimeHash == null)
				fullTimeHash = new Hashtable();
			fullTimeHash.putAll(timeHash);
			vert.setProperty("timeHash", fullTimeHash);
		}
		
	}

}
