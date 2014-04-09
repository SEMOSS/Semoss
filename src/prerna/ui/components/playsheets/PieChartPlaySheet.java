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
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/singlechartgrid.html";
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		Hashtable<String, ArrayList<Object[]>> data = new Hashtable<String, ArrayList<Object[]>>();
		ArrayList<Object[]> values = new ArrayList<Object[]>();
		for( int i = 0; i < list.size(); i++)
		{
			values.add(list.get(i));
		}
		data.put(names[1], values);
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("type", "pie");
		columnChartHash.put("dataSeries", data);
		
		return columnChartHash;
	}
}
