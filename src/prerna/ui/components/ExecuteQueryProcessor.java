package prerna.ui.components;

import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

public class ExecuteQueryProcessor {

	Logger logger = Logger.getLogger(getClass());
	boolean custom = false; 
	boolean append = false;
	IPlaySheet playSheet = null;
	
	public void setCustomBoolean (boolean custom)
	{
		this.custom = custom;
	}
	
	public void setAppendBoolean (boolean append)
	{
		this.append = append;
	}
	
	public IPlaySheet getPlaySheet()
	{
		return playSheet;
	}
	
	
	public void processCustomQuery(String engineName, String query, String playSheetString)
	{
		//get engine
		//get playsheetclassname
		//prepare create title+id
		//feed to prepare playSheet
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		String playSheetClassName = null;
		if(playSheetString.startsWith("*"))
		{
			//String keyToSearch = id + "_" + Constants.LAYOUT;
			//playSheetClassName = DIHelper.getInstance().getProperty(keyToSearch);
		}
		else //get the playsheet from play sheet enum
		{
			playSheetClassName = PlaySheetEnum.getClassFromName(playSheetString);
		}
		QuestionPlaySheetStore.getInstance().customIDcount++;
		String playSheetTitle = "Custom Query - "+QuestionPlaySheetStore.getInstance().getCustomCount();
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+"custom";
		prepareQueryOutputPlaySheet(engine, query, playSheetClassName, playSheetTitle,insightID);
	}
	
	public void processQuestionQuery(String engineName, String insightString, Hashtable<String, Object> paramHash)
	{
		//get engine
		//prepare insight and fill in paramHash into query
		//create title...need to parse out the params to add into the question title then add title...what a pain
		//create ID
		//add count to playsheetstore
		//paramFill for query
		//feed to prepare playsheet
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		Insight insight = engine.getInsight(insightString);
		String playSheetTitle = "";
		if(!paramHash.isEmpty() || paramHash==null)
		{
			Enumeration enumKey = paramHash.keys();

			while (enumKey.hasMoreElements())
			{
				String key = (String) enumKey.nextElement();
				Object value = (Object) paramHash.get(key);
				if(value instanceof String || value instanceof Double )
				{
					playSheetTitle = playSheetTitle +Utility.getInstanceName(value+"") + " - ";
				}
			}
		}
		System.out.println("Param Hash is " + paramHash);
		String[] questionTitleArray = insightString.split("\\.");
		playSheetTitle = playSheetTitle+questionTitleArray[1].trim()+" ("+questionTitleArray[0]+")";
		QuestionPlaySheetStore.getInstance().idCount++;
		String insightID = QuestionPlaySheetStore.getInstance().getIDCount()+". "+ insight.getId();
		String sparql = Utility.normalizeParam(insight.getSparql());
		System.out.println("SPARQL " + sparql);
		sparql = prerna.util.Utility.fillParam(sparql, paramHash);
		prepareQueryOutputPlaySheet(engine, sparql, insight.getOutput(), playSheetTitle,insightID);
	}

	public void prepareQueryOutputPlaySheet(IEngine engine, String sparql, String playSheetClassName, String playSheetTitle, String insightID)
	{
		System.err.println("SPARQL is " + sparql);
		//if append, dont need to set all the other playsheet stuff besides query
		if (append) {
			logger.debug("Appending ");
			playSheet = QuestionPlaySheetStore.getInstance().getActiveSheet();
			playSheet.setQuery(sparql);
		}
		else
		{
			try {
				playSheet = (IPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
			} catch (Exception ex) {
				ex.printStackTrace();
				logger.fatal("No such PlaySheet: "+ playSheetClassName);
			}
			QuestionPlaySheetStore.getInstance().put(insightID,  playSheet);
			playSheet.setQuery(sparql);
			playSheet.setRDFEngine(engine);
			playSheet.setQuestionID(insightID);
			playSheet.setTitle(playSheetTitle);
		}


	}

}
