package prerna.ui.components.specific.tap;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.bigdata.rdf.model.BigdataURIImpl;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.playsheets.AbstractRDFPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SystemTransitionReport extends AbstractRDFPlaySheet{

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
			logger.info("Creating System Transition Report for system >>> "+sysURI);
			IndividualSystemTransitionReport sysTransReport = new IndividualSystemTransitionReport();
			sysTransReport.enableMessages(false);
			sysTransReport.setQuery(sysURI);
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
	/**
	 * Sets the string version of the SPARQL query on the playsheet. 
	 * @param query String
	 */
/*	@Override
	public void setQuery(String query) 
	{
		if(query.startsWith("SELECT")||query.startsWith("CONSTRUCT"))
			this.query=query;
		else
		{
			logger.info("Query " + query);
			int selectIndex = query.indexOf("$");
			String systemTypesResponse = query.substring(0,selectIndex);
			query = query.substring(selectIndex+1);

			if(systemTypesResponse.equals("LPI"))
			{
				this.isLPIReport = true;
			} else {
				this.isLPIReport = false;
			}

			logger.info("New Query " + query);
			this.query = query;
		}
	}*/

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
