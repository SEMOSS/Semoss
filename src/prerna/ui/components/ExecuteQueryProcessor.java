package prerna.ui.components;

import java.lang.reflect.InvocationTargetException;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.util.DIHelper;
import prerna.util.PlaySheetEnum;
import prerna.util.QuestionPlaySheetStore;
import prerna.util.Utility;

public class ExecuteQueryProcessor {

	static final Logger logger = LogManager.getLogger(ExecuteQueryProcessor.class.getName());
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

	public void setPlaySheet(IPlaySheet playSheet){
		this.playSheet = playSheet;
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
		
		//get the playsheet from play sheet enum
		String playSheetClassName = PlaySheetEnum.getClassFromName(playSheetString);
		if(playSheetClassName.isEmpty()){ //this will happen if it is a custom playsheet that does not exist in the enum. In this case, we already have the full playsheet name from the name of the combobox
			playSheetClassName = playSheetString;
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
		Insight insight = ((AbstractEngine)engine).getInsight2(insightString).get(0);
		String playSheetTitle = "";
		if(paramHash!=null || !paramHash.isEmpty())
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
			if(playSheet==null)
				playSheet = QuestionPlaySheetStore.getInstance().getActiveSheet();
			if(playSheet==null)
				Utility.showError("Cannot overlay data without selected playsheet");
			playSheet.setQuery(sparql);
			playSheet.setRDFEngine(engine);
		}
		else
		{
			try {
				playSheet = (IPlaySheet) Class.forName(playSheetClassName).getConstructor(null).newInstance(null);
			} catch (ClassNotFoundException ex) {
				ex.printStackTrace();
				logger.fatal("No such PlaySheet: "+ playSheetClassName);
			} catch (InstantiationException e) {
				e.printStackTrace();
				logger.fatal("No such PlaySheet: "+ playSheetClassName);
			} catch (IllegalAccessException e) {
				logger.fatal("No such PlaySheet: "+ playSheetClassName);
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				logger.fatal("No such PlaySheet: "+ playSheetClassName);
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				logger.fatal("No such PlaySheet: "+ playSheetClassName);
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				logger.fatal("No such PlaySheet: "+ playSheetClassName);
				e.printStackTrace();
			} catch (SecurityException e) {
				logger.fatal("No such PlaySheet: "+ playSheetClassName);
				e.printStackTrace();
			}
			QuestionPlaySheetStore.getInstance().put(insightID,  playSheet);
			playSheet.setQuery(sparql);
			playSheet.setRDFEngine(engine);
			playSheet.setQuestionID(insightID);
			playSheet.setTitle(playSheetTitle);
		}


	}
	
}
