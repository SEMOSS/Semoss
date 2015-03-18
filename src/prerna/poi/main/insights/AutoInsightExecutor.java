/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.poi.main.insights;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.swing.JComboBox;
import javax.swing.JList;

import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.rdf.engine.impl.QuestionAdministrator;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * AutoInsightGenerator creates insights that are automatically added to a DB on upload
 * @author ksmart
 *
 */
public class AutoInsightExecutor {

	//engine and question administrator to add questions
	private AbstractEngine engine;
	private QuestionAdministrator qa;

	//concepts and perspectives + number of questions in perspective
	private final String CONCEPTS_QUERY = "SELECT DISTINCT ?Concept WHERE {{?Concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>}}";

	public AutoInsightExecutor(AbstractEngine engine) {
		this.engine = engine;
	}

	public void run() {
		
		long startTime = System.currentTimeMillis();
		
		qa = new QuestionAdministrator(engine);
		Vector<String> perspectives = engine.getPerspectives();
		Hashtable<String, Integer> perspectiveHash = new Hashtable<String,Integer>();
		for(String perspective : perspectives){
			perspectiveHash.put(perspective, engine.getInsights(perspective).size() + 1);
		}

		InsightTemplateProcessor templateProc = new InsightTemplateProcessor();
		List<InsightRule> rulesList = templateProc.runGenerateInsights();

		//TODO how to filter the list of concepts?
		ArrayList<String> concepts = getQueryResultsAsList(CONCEPTS_QUERY);
		concepts.remove("Concept");

		ExecutorService executor = Executors.newFixedThreadPool(8);
		int i;
		int numConcepts = concepts.size();
		for(i=0; i<numConcepts; i++) {
			AutoInsightRunnable thread = new AutoInsightRunnable(engine, qa, concepts.get(i), rulesList, perspectiveHash);
			executor.submit(thread);
		}
		
		executor.shutdown();
		while(!executor.isTerminated()) {
			// wait till all threads are done processing
		}
		
		//reload the perspectives for the db so that new perspectives/questions are visible
//		reloadDB();
		
		long endTime = System.currentTimeMillis();
		System.out.println("Time in sec: " + (endTime - startTime)/1000 );
	}
		
	private ArrayList<String> getQueryResultsAsList(String query) {

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		// get the bindings from it
		String[] names = wrapper.getVariables();
		ArrayList<String> properties = new ArrayList<String>();

		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			ISelectStatement sjss = wrapper.next();
			properties.add(sjss.getVar(names[0]).toString());
		}

		return properties;
	}

	private void reloadDB() {
		// selects the db in repolist so the questions refresh with the changes
		JList list = (JList) DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		List selectedList = list.getSelectedValuesList();
		String selectedValue = selectedList.get(selectedList.size() - 1).toString();

		// don't need to refresh if selected db is not the db you're modifying.
		// when you click to it it will refresh anyway.
		if (engine.getEngineName().equals(selectedValue)) {
			Vector<String> perspectives = engine.getPerspectives();
			Collections.sort(perspectives);

			JComboBox<String> box = (JComboBox<String>) DIHelper.getInstance().getLocalProp(Constants.PERSPECTIVE_SELECTOR);
			box.removeAllItems();

			for (int itemIndex = 0; itemIndex < perspectives.size(); itemIndex++) {
				box.addItem(perspectives.get(itemIndex).toString());
			}
		}
	}

}
