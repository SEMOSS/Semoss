package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.util.Hashtable;

import prerna.ui.components.playsheets.BrowserPlaySheet;

public class DataProvenanceHeatMapSheet extends BrowserPlaySheet {
	
	/**
	 * Constructor for HeatMapPlaySheet.
	 */
	public DataProvenanceHeatMapSheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = System.getProperty("user.dir");
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/dataprovenance.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Consists of the x-value, y-value, x- and y-axis titles, and the title of the map.*/
	public Hashtable processQueryData()
	{
		Hashtable dataHash = new Hashtable();
		Hashtable dataSeries = new Hashtable();
		String[] var = wrapper.getVariables();
		String xName = var[0];
		String yName = var[1];
		for (int i=0;i<list.size();i++)
		{
			Hashtable elementHash = new Hashtable();
			Object[] listElement = list.get(i);			
			String methodName = (String) listElement[0];
			String groupName = (String) listElement[1];
			methodName = methodName.replaceAll("\"", "");
			groupName = groupName.replaceAll("\"", "");
			String key = methodName +"-"+groupName;
			String crm = (String) listElement[2];
			crm = crm.replaceAll("\"", "");
			double count = 1;
			elementHash.put(xName, methodName);
			elementHash.put(yName, groupName);
			if (crm.equals("C")) {
				count = 1;
			}
			else if (crm.equals("R") && !elementHash.containsKey(var[2])) {
				count = 0;
			}			
			elementHash.put(var[2], count);
			dataHash.put(key, elementHash);
			
		}

		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		allHash.put("title",  var[0] + " vs " + var[1]);
		allHash.put("xAxisTitle", var[0]);
		allHash.put("yAxisTitle", var[1]);
		allHash.put("value", var[2]);
		
		return allHash;
	}
	

}
