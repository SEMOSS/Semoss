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

		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			for( int j = 1; j < elemValues.length; j++)
			{
				ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
				if(dataObj.size() >= j)
					seriesArray = dataObj.get(j-1);
				else
					dataObj.add(j-1, seriesArray);
				Hashtable<String, Object> elementHash = new Hashtable<String, Object>();
				String xvalue = elemValues[0].toString();
				Double xVal = null;
				try {
					xVal = Double.parseDouble(xvalue);
					elementHash.put("x", xVal);
				}
				catch (NumberFormatException e) {
					elementHash.put("x", xvalue);
				}				
				elementHash.put("y", elemValues[1]);
				if (elemValues.length > 2) {
					elementHash.put("x1", elemValues[2]);
					elementHash.put("y1", elemValues[3]);
				}
				seriesArray.add(elementHash);
			}
		}
		
		Hashtable<String, Object> lineChartHash = new Hashtable<String, Object>();
		lineChartHash.put("names", names);
		lineChartHash.put("type", "line");
		lineChartHash.put("dataSeries", dataObj);
		
		return lineChartHash;
	}
}

