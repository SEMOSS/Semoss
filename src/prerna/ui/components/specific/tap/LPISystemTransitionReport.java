package prerna.ui.components.specific.tap;

import org.apache.log4j.Logger;

import com.bigdata.rdf.model.BigdataURIImpl;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class LPISystemTransitionReport extends AbstractRDFPlaySheet{

	Logger logger = Logger.getLogger(getClass());
	private IEngine hr_Core;
	
	@Override
	public void createData() {
		try{
			hr_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		} catch(Exception e) {
			e.printStackTrace();
			Utility.showError("Could not find necessary databases:\nHR_Core, TAP_Cost_Data");
		}
		
		SesameJenaSelectWrapper sjsw = processQuery(hr_Core, query);
		String[] names = sjsw.getVariables();
		
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			BigdataURIImpl sysRawURI = (BigdataURIImpl)sjss.getRawVar(names[0]);
			String sysURI = "<"+sysRawURI.stringValue()+">";
			logger.info("Creating LPI System Transition Report for system >>> "+sysURI);
			IndividualSystemTransitionReport lpiReport = new IndividualSystemTransitionReport();
			lpiReport.enableMessages(false);
			lpiReport.setQuery(sysURI);
			lpiReport.createData();
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
