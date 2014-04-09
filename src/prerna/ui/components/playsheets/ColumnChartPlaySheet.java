package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class ColumnChartPlaySheet extends BrowserPlaySheet{

	public ColumnChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singlechartgrid.html";
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		Hashtable<String, ArrayList<Object>> data = new Hashtable<String, ArrayList<Object>>();
		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			ArrayList<Object> values = new ArrayList<Object>();
			for( int j = 1; j < elemValues.length; j++)
			{
				values.add(elemValues[j]);
			}
			data.put(elemValues[0].toString(), values);	
		}
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("xAxis", Arrays.copyOfRange(names, 1, names.length));
		columnChartHash.put("type", "column");
		columnChartHash.put("dataSeries", data);
		
		return columnChartHash;
	}
	
}
