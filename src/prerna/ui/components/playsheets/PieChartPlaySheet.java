package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Hashtable;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class PieChartPlaySheet extends BrowserPlaySheet{

	public PieChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/piechart.html";
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
				Hashtable<String, Object> elementHash = new Hashtable();
				elementHash.put("pieCat", elemValues[0].toString());
				elementHash.put("pieVal", elemValues[j]);
				seriesArray.add(elementHash);
			}
		}
		
		Hashtable<String, Object> pieChartHash = new Hashtable<String, Object>();
		pieChartHash.put("names", names);
		pieChartHash.put("type", "pie");
		pieChartHash.put("dataSeries", dataObj);
		
		return pieChartHash;
	}
}
