package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class LineChartPlaySheet extends BrowserPlaySheet{

	public LineChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/linechart.html";
	}	
	
	public Hashtable<String, Object> processQueryData()
	{		
		ArrayList< ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList< ArrayList<Hashtable<String, Object>>>();
		
		for(int i = 0; i < names.length; i = i + 2) {
			ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
			Hashtable<String, Object> seriesHash = new Hashtable<String, Object>();
			ArrayList<Hashtable<String,Object>> dataArray = new ArrayList<Hashtable<String,Object>>();
			
			for(int j = 0; j < list.size(); j++) {
				Object[] elemValues = list.get(j);			
				
				Hashtable<String, Object> elementHash = new Hashtable<String, Object>();
				elementHash.put("x", elemValues[i]);
				elementHash.put("y", elemValues[i+1]);
				dataArray.add(elementHash);
			}
			seriesHash.put("xName", names[i]);
			seriesHash.put("yName", names[i+1]);
			seriesHash.put("dataPoints", dataArray);
			seriesArray.add(seriesHash);
			dataObj.add(seriesArray);
		}
				
		Hashtable<String, Object> lineChartHash = new Hashtable<String, Object>();
		lineChartHash.put("names", names);
		lineChartHash.put("type", "line");
		lineChartHash.put("dataSeries", dataObj);
		
		return lineChartHash;
	}
}

