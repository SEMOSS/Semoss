package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class DrillDownPlaySheet extends BrowserPlaySheet {

	public DrillDownPlaySheet() {
		// TODO Auto-generated constructor stub
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/drilldown.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Consists of the x-value, y-value, x- and y-axis titles, and the title of the map.*/
	public Hashtable processQueryData()
	{
		
		Hashtable dataHash = new Hashtable();
		String[] var = wrapper.getVariables();
		
		for (int i = 1;i<var.length;i++) {
			Hashtable<String,ArrayList<String>> elementHash = new Hashtable<String,ArrayList<String>>();
			
			for (int j=0; j<list.size();j++) {
				ArrayList<String> listElementArray = new ArrayList<String>();
				Object[] listElement = list.get(j);
				
				if (listElement[i]== null || ((String)listElement[i]).isEmpty()) continue;
				
				String systemService = (String) listElement[0];
			
				if (elementHash.get(systemService) != null) {
					listElementArray = elementHash.get(systemService);
				}
				
				
				listElementArray.add((String)listElement[i]);
				
			
				
				elementHash.put(systemService, listElementArray);
				
			}
			
				String key = var[i];
				dataHash.put(key, elementHash);
			
				
		}
			

		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);		
		allHash.put("xAxisTitle", "Node Type");
		allHash.put("yAxisTitle", "System Service");
		
		
		return allHash;
	}
	

}
