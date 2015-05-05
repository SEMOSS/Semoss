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
import java.util.Hashtable;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.engine.impl.AbstractEngine;
import prerna.engine.impl.QuestionAdministrator;
import prerna.om.Insight;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.DIHelper;

/**
 * AutoInsightGenerator creates insights that are automatically added to a DB on upload
 * @author ksmart
 *
 */
public class AutoInsightExecutor implements Runnable{

	//engine and question administrator to add questions
	private AbstractEngine engine;
	private QuestionAdministrator qa;
	private boolean success;
	
	//concepts and perspectives + number of questions in perspective
	private final String CONCEPTS_QUERY = "SELECT DISTINCT ?Concept WHERE {{?Concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>}}";

	public AutoInsightExecutor(AbstractEngine engine) {
		this.engine = engine;
	}
	
	public AutoInsightExecutor(String engineName) {
		this.engine = (AbstractEngine) DIHelper.getInstance().getLocalProp(engineName);
	}

	@Override
	public void run(){
		success = true;
		
		long startTime = System.currentTimeMillis();
		
		qa = new QuestionAdministrator(engine);
		Vector<String> perspectives = engine.getPerspectives();
		Hashtable<String, Integer> perspectiveIDHash = new Hashtable<String, Integer>();
		
		for(String perspective : perspectives) {
			// calculate the max id to use
			int maxId = 0;
			Vector<String> questionList = engine.getInsights(perspective);
			for(String question : questionList) {
				String questString = question;
				Vector<Insight> in = engine.getInsight2(questString);
				Insight insight = in.get(0);
				String questionId = insight.getId();
				int id = Integer.parseInt(questionId.split(":")[2].replaceAll("\\D", ""));
				if(id > maxId) {
					maxId = id;
				}
			}
			perspectiveIDHash.put(perspective, maxId);
		}

		InsightTemplateProcessor templateProc = new InsightTemplateProcessor();
		List<InsightRule> rulesList = templateProc.runGenerateInsights();

		//TODO how to filter the list of concepts?
		ArrayList<String> concepts = getQueryResultsAsList(CONCEPTS_QUERY);
		concepts.remove("Concept");

		ExecutorService executor = Executors.newCachedThreadPool();
		int i;
		int numConcepts = concepts.size();
		List<Future<List<Object[]>>> futureList = new ArrayList<Future<List<Object[]>>>();
		ListIterator<Future<List<Object[]>>> futureListIt = futureList.listIterator();
		for(i = 0; i < numConcepts; i++) {
			AutoInsightCallable thread = new AutoInsightCallable(engine, qa, concepts.get(i), rulesList, perspectiveIDHash);
			Future<List<Object[]>> future = executor.submit(thread);
			futureListIt.add(future);
		}
		
		executor.shutdown();
		while(!executor.isTerminated()) {
			// wait till all threads are done processing
			// start adding insights as they are added
			try {
				addInsightsToXML(futureListIt);
			} catch (InterruptedException e) {
				e.printStackTrace();
				success = false;
			} catch (ExecutionException e) {
				e.printStackTrace();
				success = false;
			}
		}
		
		String xmlFile = "db/" + engine.getEngineName() + "/" + engine.getEngineName() + "_Questions.XML";
		String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");
		qa.createQuestionXMLFile(xmlFile, baseFolder);
		
		long endTime = System.currentTimeMillis();
		System.out.println("Time in sec: " + (endTime - startTime)/1000 );
	}
	
	private void addInsightsToXML(ListIterator<Future<List<Object[]>>> futureListIt) throws InterruptedException, ExecutionException {
		while(futureListIt.hasPrevious()) {
			Future<List<Object[]>> future = futureListIt.previous();
			List<Object[]> insights = null;
			insights = future.get();
			AutoInsightCallable.addInsightsToXML(insights);
			futureListIt.remove();
		}
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
	
	public boolean isSuccess() {
		return success;
	}
}
