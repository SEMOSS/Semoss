/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.ui.main.listener.impl;

import java.awt.event.ActionEvent;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import javax.swing.JComponent;
import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.InMemoryJenaEngine;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.playsheets.GridPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;

/**
 * Controls the export graph to grid feature.
 */
public class GraphPlaySheetExportListener  extends AbstractListener{

	public static GraphPlaySheetExportListener listener = null;
	static final Logger logger = LogManager.getLogger(GraphPlaySheetExportListener.class.getName());
	GraphPlaySheet playSheet;

	/**
	 * Method getInstance.  Retrieves the instance of the graph play sheet export listener.
	
	 * @return GraphPlaySheetExportListener */
	public static GraphPlaySheetExportListener getInstance()
	{
		if(listener == null)
			listener = new GraphPlaySheetExportListener();
		return listener;
	}
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param arg0 ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent arg0) {
		logger.info("Export button has been pressed");
		GraphPlaySheet playSheet=  (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();

		GridPlaySheet NewplaySheet = new GridPlaySheet();
//		try {
//			NewplaySheet = (GridPlaySheet)Class.forName("prerna.ui.components.playsheets.GridPlaySheet").getConstructor(null).newInstance(null);
//		}catch(Exception ex)
//		{
//			ex.printStackTrace();
//			logger.fatal(ex);
//		}
		playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		String query = playSheet.getQuery();
		String selectQuery = convertConstructToSelect(query);
		String questionID = playSheet.getQuestionID();
		String question = QuestionPlaySheetStore.getInstance().getIDCount() + ". "+questionID;
		String title = "Grid Export - " + playSheet.getTitle();
		IEngine engine = playSheet.getRDFEngine();
		
		InMemoryJenaEngine jenaEng = new InMemoryJenaEngine();
		Model jenaModel = playSheet.getGraphData().getJenaModel();
		jenaEng.setModel(jenaModel);
		
		
		String sparql = "SELECT ?Subjects ?Predicates ?Objects WHERE {?Subjects ?Predicates ?Objects}";		
		ResultSet rs = (ResultSet) jenaEng.execSelectQuery(sparql);
		NewplaySheet.setRs(rs);
		
		NewplaySheet.setTitle(title);
		NewplaySheet.setQuestionID(question);
		JDesktopPane pane = (JDesktopPane)DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		NewplaySheet.setJDesktopPane(pane);
	
		// put it into the store
		QuestionPlaySheetStore.getInstance().put(question, playSheet);
		
		// thread
		PlaysheetCreateRunner playThread = new PlaysheetCreateRunner(NewplaySheet);
		playThread.run();
			
		String tabTitle = "Graph Export";
		playSheet.jTab.add(tabTitle, NewplaySheet);
		playSheet.jTab.setSelectedComponent(NewplaySheet);
	}
		

	/**
	 * Method convertConstructToSelect.  Converts the construct query to a select query.
	 * @param ConstructQuery String  The construct query to be converted.
	
	 * @return String the converted select query.*/
	public String convertConstructToSelect(String ConstructQuery){
		logger.info("Begining to convert query");
		String result = null;
		String newConstructQuery = null;
		
		//need to separate {{ so that tokenizer gets every piece of the construct query
		if(ConstructQuery.contains("{{")) newConstructQuery = ConstructQuery.replaceAll(Pattern.quote("{{"), "{ {");
		else newConstructQuery = ConstructQuery;
		StringTokenizer QueryBracketTokens = new StringTokenizer(newConstructQuery, "{");
		//Everything before first { becomes SELECT
		String firstToken = QueryBracketTokens.nextToken();
		String newFirstToken = null;
		if (firstToken.toUpperCase().contains("CONSTRUCT")){
			newFirstToken = firstToken.toUpperCase().replace("CONSTRUCT", "SELECT DISTINCT ");
		}
		else logger.info("Converting query that is not CONSTRUCT....");
		
		//the second token coming from between the first two brackets must have all period and semicolons removed
		String secondToken = QueryBracketTokens.nextToken();
		String newsecondToken = null;
		if(secondToken.contains(";") && secondToken.contains(".")){
			String secondTokensansSemi = secondToken.replaceAll(";", " ");
			String secondTokensansPer = secondTokensansSemi.replaceAll(".", " ");
			newsecondToken = secondTokensansPer.replace("}", " ");
		}
		else if(secondToken.contains(".")&&!secondToken.contains(";")){
			String secondTokensansPer = secondToken.replaceAll("\\.", " ");
			newsecondToken = secondTokensansPer.replace("}", " ");
		}
		else if(secondToken.contains(";")&&!secondToken.contains(".")){
			String secondTokensansSemi = secondToken.replaceAll(";", " ");
			newsecondToken = secondTokensansSemi.replace("}", " ");
		}
		else if(!secondToken.contains(";")&&!secondToken.contains(".")){
			String secondTokensansSemi = secondToken.replaceAll(";", " ");
			newsecondToken = secondTokensansSemi.replace("}", " ");
		} 
		
		
		// Rest of the tokens go unchanged.  Must iterate through and replace {
		String restOfQuery = "";
		while (QueryBracketTokens.hasMoreTokens()){
			String token = QueryBracketTokens.nextToken();
			restOfQuery = restOfQuery+"{"+token;
		}
		result = newFirstToken + newsecondToken +restOfQuery;
		return result;
	}

	/**
	 * Method setPlaysheet.  Sets the play sheet that the listener will access.
	 * @param ps IPlaySheet
	 */
	public void setPlaysheet(IPlaySheet ps) {
		playSheet = (GraphPlaySheet) ps;
		
	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		
	}

}
