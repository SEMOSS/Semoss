package prerna.ui.components.specific.tap;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Hashtable;

import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;

@SuppressWarnings("serial")
public class DHMSMSysDecommissionPlaySheet extends BrowserPlaySheet{

	private DHMSMSysDecommissionReport data;
	
	
	//TODO: this class should not extend BrowserPlaySheet and should create new methods to send information to JS
	public void setData(DHMSMSysDecommissionReport data) {
		this.data = data;
		this.list = new ArrayList<Object[]>();
		this.list.add(new Object[1]);
		this.names = new String[1];
	}

	/**
	 * Constructor for DHMSMSysDecommissionPlaySheet.
	 */
	public DHMSMSysDecommissionPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/world-map-timeline.html";
	}
	
	@Override
	public void createData()
	{
		if(data == null || data.masterHash == null)
		{
			data = new DHMSMSysDecommissionReport();
			data.runCalculation();
		}
		
		DHMSMSysDecommissionDataProcessing dataProcessing = new DHMSMSysDecommissionDataProcessing();
		dataProcessing.setDataSource(data);
		dataProcessing.setAllData(data.masterHash);
		dataProcessing.decomposeData();
		Hashtable<Integer, Object> allHash = dataProcessing.constructHash();
		
		this.dataHash = allHash;
	}
}
