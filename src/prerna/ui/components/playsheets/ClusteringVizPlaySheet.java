package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.impl.ClusteringAlgorithm;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.Constants;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class ClusteringVizPlaySheet extends BrowserPlaySheet{

	private int numClusters;
	
	public ClusteringVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/cluster.html";
	}
	
	@Override
	public Hashtable processQueryData()
	{
		ArrayList<Hashtable<String, Object>> dataList = new ArrayList<Hashtable<String, Object>>();
		for(Object[] dataRow : list) {
			Hashtable<String, Object> instanceHash = new Hashtable<String, Object>();
			// add name and cluster under special names first
			instanceHash.put("NodeName", dataRow[0]);
			instanceHash.put("ClusterID", dataRow[dataRow.length-1]);			
			//loop through properties and add to innerHash
			for(int i = 1; i < dataRow.length - 1; i++) {
				instanceHash.put(names[i], dataRow[i]);
			}
			dataList.add(instanceHash);
		}
		
		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataList);
		return allHash;
	}
	
	@Override
	public void createData() {
		processQuery();
		ClusteringAlgorithm clusterAlg = new ClusteringAlgorithm(list,names);
		clusterAlg.setNumClusters(numClusters);
		clusterAlg.execute();
		ArrayList<Integer> clusterAssigned = clusterAlg.getClustersAssigned();
		Hashtable<String, Integer> instanceIndexHash = clusterAlg.getInstanceIndexHash();
		ArrayList<Object[]> newList = new ArrayList<Object[]>();
		for(Object[] dataRow : list) {
			Object[] newDataRow = new Object[dataRow.length + 1];
			String instance = "";
			for(int i = 0; i < dataRow.length; i++) {
				if(i == 0) {
					instance = dataRow[i].toString();
				}
				newDataRow[i] = dataRow[i];
			}
			Integer clusterNumber = clusterAssigned.get(instanceIndexHash.get(instance));
			newDataRow[dataRow.length] = clusterNumber;
			newList.add(newDataRow);
		}
		list = newList;
		String[] newNames = new String[names.length + 1];
		for(int i = 0; i < names.length; i++) {
			newNames[i] = names[i];
		}
		newNames[names.length] = "CluserID";
		names = newNames;
		
		dataHash = processQueryData();
	}
	
	private void processQuery() 
	{
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		names = sjsw.getVariables();
		list = new ArrayList<Object[]>();
		
		while(sjsw.hasNext()) {
			SesameJenaSelectStatement sjss = sjsw.next();
			Object[] dataRow = new Object[names.length];
			for(int i = 0; i < names.length; i++) {
				dataRow[i] = sjss.getVar(names[i]);
			}
			list.add(dataRow);
		}
	}

	
	/**
	 * Sets the string version of the SPARQL query on the playsheet.
	 * Pulls out the number of clusters and stores them in the numClusters
	 * @param query String
	 */
	@Override
	public void setQuery(String query) {
		if(query.startsWith("SELECT") || query.startsWith("CONSTRUCT"))
			this.query=query;
		else{
			logger.info("New Query " + query);
			int semi=query.indexOf(";");
			numClusters = Integer.parseInt(query.substring(0,semi));
			this.query = query.substring(semi+1);
		}
	}
}
