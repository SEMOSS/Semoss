package prerna.ui.components.specific.tap;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

import com.bigdata.rdf.model.BigdataURIImpl;

@SuppressWarnings("serial")
public class SystemTransitionReport extends AbstractRDFPlaySheet{

	Logger logger = Logger.getLogger(getClass());
	private IEngine hr_Core;

	@Override
	public void createData() {
		try{
			hr_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
			if(hr_Core==null)
				throw new Exception();
		} catch(Exception e) {
			Utility.showError("Could not find necessary database: HR_Core. Cannot generate report.");
			return;
		}

		String[] systemAndReport = query.split("\\$");
		this.query = systemAndReport[0];
		String reportType = systemAndReport[1];

		SesameJenaSelectWrapper sjsw = processQuery(hr_Core, systemAndReport[0]);

		String[] names = sjsw.getVariables();
		ArrayList<String> systemList = new ArrayList<String>();
		String systemListString = "";
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			BigdataURIImpl sysRawURI = (BigdataURIImpl)sjss.getRawVar(names[0]);
			String sysURI = "<"+sysRawURI.stringValue()+">";
			systemList.add(sysURI);
			systemListString+= sysURI + " ";
		}
		logger.info("Creating System Transition Reports for systems >>> "+systemListString);

		for(int i=0;i<systemList.size();i++)
		{
			String sysURI = systemList.get(i);
			logger.info("Creating System Transition Report for system >>> " + sysURI + "$" + reportType);
			IndividualSystemTransitionReport sysTransReport = new IndividualSystemTransitionReport();
			sysTransReport.enableMessages(false);
			sysTransReport.setQuery(sysURI + "$" + reportType);
			sysTransReport.createData();
		}

		String fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\export\\Reports\\";
		Utility.showMessage("System Transition Reports Finished! Files located in:\n" +fileLoc);
	}

	private SesameJenaSelectWrapper processQuery(IEngine engine, String query){
		logger.info("PROCESSING QUERY: " + query);
		SesameJenaSelectWrapper sjsw = new SesameJenaSelectWrapper();
		//run the query against the engine provided
		sjsw.setEngine(engine);
		sjsw.setQuery(query);
		sjsw.executeQuery();	
		return sjsw;
	}

	@Override
	public void refineView() {
		// TODO Auto-generated method stub
	}

	@Override
	public void overlayView() {
		// TODO Auto-generated method stub
	}

	@Override
	public void runAnalytics() {
		// TODO Auto-generated method stub
	}

}
