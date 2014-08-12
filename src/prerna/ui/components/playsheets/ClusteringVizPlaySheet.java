package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.impl.ClusteringAlgorithm;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.GridTableRowSorter;
import prerna.ui.components.NewScrollBarUI;
import prerna.util.Constants;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class ClusteringVizPlaySheet extends BrowserPlaySheet{

	private static final Logger logger = LogManager.getLogger(ClusteringVizPlaySheet.class.getName());
	private int numClusters;
	ArrayList<Object[]> clusterInfo;
	
	public ClusteringVizPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/cluster.html";
	}
	
	@Override
	public void createView() {
		super.createView();
		JPanel panel = new JPanel();
		table = new JTable();
		panel.add(table);
		GridBagLayout gbl_mainPanel = new GridBagLayout();
		gbl_mainPanel.columnWidths = new int[]{0, 0};
		gbl_mainPanel.rowHeights = new int[]{0, 0};
		gbl_mainPanel.columnWeights = new double[]{1.0, Double.MIN_VALUE};
		gbl_mainPanel.rowWeights = new double[]{1.0, Double.MIN_VALUE};
		panel.setLayout(gbl_mainPanel);
		addScrollPanel(panel);
		GridFilterData gfd = new GridFilterData();
		list.addAll(0, clusterInfo);
		gfd.setColumnNames(names);
		//append cluster information to list data
		gfd.setDataList(list);
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setRowSorter(new GridTableRowSorter(model));

		jTab.addTab("Raw Data", panel);
	}
	
	
	@Override
	public Hashtable processQueryData()
	{
		ArrayList<Hashtable<String, Object>> dataList = new ArrayList<Hashtable<String, Object>>();
		for(Object[] dataRow : list) {
			Hashtable<String, Object> instanceHash = new Hashtable<String, Object>();
			// add name and cluster under special names first
			instanceHash.put("ClusterID", dataRow[dataRow.length - 1]);			
			instanceHash.put("NodeName", dataRow[0]);
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
		//store cluster final state information
		clusterInfo = new ArrayList<Object[]>(numClusters);
		clusterInfo = clusterAlg.getClusterRows();
		//iterate through query return
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
			newDataRow[newDataRow.length - 1] = clusterNumber;
			newList.add(newDataRow);
			//add to matrix
		}
		list = newList;
		String[] newNames = new String[names.length + 1];
		for(int i = 0; i < names.length; i++) {
			newNames[i] = names[i];
		}
		newNames[newNames.length - 1] = "CluserID";
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
	
	public void addScrollPanel(JPanel rawDataPanel) {
		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);
		
		GridBagConstraints gbc_scrollPane = new GridBagConstraints();
		gbc_scrollPane.fill = GridBagConstraints.BOTH;
		gbc_scrollPane.gridx = 0;
		gbc_scrollPane.gridy = 0;
		rawDataPanel.add(scrollPane, gbc_scrollPane);
	}
}
