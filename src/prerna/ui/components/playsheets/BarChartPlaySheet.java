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
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Set;

import javax.swing.JButton;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.inferencer.fc.ForwardChainingRDFSInferencer;
import org.openrdf.sail.memory.MemoryStore;

import prerna.om.GraphDataModel;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.util.StatementCollector;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.main.listener.impl.ColumnChartGroupedStackedListener;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class BarChartPlaySheet extends BrowserPlaySheet{

	private static final Logger logger = LogManager.getLogger(BarChartPlaySheet.class.getName());
	GraphDataModel gdm = new GraphDataModel();
	
	public BarChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/barchart.html";
	}
	
	@Override
	public void createControlPanel(){
		controlPanel = new ChartControlPanel();
		controlPanel.addExportButton(1);
		
		ChartControlPanel ctrlPanel = this.getControlPanel();
		GridBagConstraints gbc_btnGroupedStacked = new GridBagConstraints();
		gbc_btnGroupedStacked.insets = new Insets(10, 0, 0, 5);
		gbc_btnGroupedStacked.anchor = GridBagConstraints.EAST;
		gbc_btnGroupedStacked.gridx = 0;
		gbc_btnGroupedStacked.gridy = 0;
		JButton transitionGroupedStacked = new JButton("Transition Grouped");
		ColumnChartGroupedStackedListener gsListener = new ColumnChartGroupedStackedListener();
		gsListener.setBrowser(this.browser);
		transitionGroupedStacked.addActionListener(gsListener);
		ctrlPanel.add(transitionGroupedStacked, gbc_btnGroupedStacked);

		this.controlPanel.setPlaySheet(this);
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		ArrayList< ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList< ArrayList<Hashtable<String, Object>>>();
		//series name - all objects in that series (x : ... , y : ...)
		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			for( int j = 1; j < elemValues.length; j++)
			{
				ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
				if(dataObj.size() >= j)
					seriesArray = dataObj.get(j-1);
				else
					dataObj.add(j-1, seriesArray);
				Hashtable<String, Object> elementHash = new Hashtable();
				elementHash.put("x", elemValues[0].toString());
				elementHash.put("y", elemValues[j]);
				elementHash.put("seriesName", names[j]);
				seriesArray.add(elementHash);
			}
		}
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("dataSeries", dataObj);
		
		return columnChartHash;
	}
	

	@Override
	public void createData()
	{
		basicProcessingCreateData();
		dataHash = processQueryData();
	}
	
	private void basicProcessingCreateData() {
		// TODO Auto-generated method stub
		// the create view needs to refactored to this
		
		if(this.overlay)
			list = gfd.dataList;
		else list = new ArrayList();
		wrapper = new SesameJenaSelectWrapper();
		if(engine!= null && rs == null){

			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			try{
				wrapper.executeQuery();	
			}
			catch (RuntimeException e)
			{
				e.printStackTrace();
			}		

		}
		else if (engine==null && rs!=null){
			wrapper.setResultSet(rs);
			wrapper.setEngineType(IEngine.ENGINE_TYPE.JENA);
		}

		StatementCollector collector = parseQuery(query);
		StringBuffer subjects = collector.getSubjectURIstring();
		StringBuffer predicates = collector.getPredicateURIstring();
		StringBuffer objects = collector.getObjectURIstring();
		Set<String> subjectVars = collector.getSubjectVariables();
		Set<String> predicateVars = collector.getPredicateVariables();
		Set<String> objectVars = collector.getObjectVariables();
		
		// get the bindings from it
		names = wrapper.getVariables();
		int count = 0;
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				
				Object [] values = new Object[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++)
				{
					Object literalValue = addToSPObuffers(names[colIndex], sjss, subjects, predicates, objects, subjectVars, predicateVars, objectVars);
					values[colIndex] = literalValue;
					logger.debug("Binding Name " + names[colIndex]);
					logger.debug("Binding Value " + values[colIndex]);
					
				}
				logger.debug("Creating new Value " + values);
				list.add(count, values);
				count++;
			}
		} catch (RuntimeException e) {
			logger.fatal(e);
		}
		
		// everything should be set now to create rc
		// going to create the rc using rdf engine helper
		// use the rc to determine what is a node or edge
		// this will be passed through monolith to determine what is clickable
		System.out.println("Subjects : " + subjects.toString());
		System.out.println("Predicates : " + predicates.toString());
		System.out.println("Objects : " + objects.toString());
		

		Repository myRepository = new SailRepository(
	            new ForwardChainingRDFSInferencer(
	            new MemoryStore()));
		try {
			myRepository.initialize();
			gdm.rc = myRepository.getConnection();	
			gdm.rc.setAutoCommit(false);
		} catch (RepositoryException e) {
			logger.error(e);
		}
		gdm.loadBaseData(engine);
		
		logger.info("BaseQuery");
		String containsRelation = gdm.findContainsRelation();
		if(containsRelation == null)
			containsRelation = "<http://semoss.org/ontologies/Relation/Contains>";
		// load the concept linkages
		// the concept linkages are a combination of the base relationships and what is on the file
		boolean loadHierarchy = !(subjects.length()==0 && predicates.length()==0 && objects.length()==0); 
		if(loadHierarchy) {
			try {
				RDFEngineHelper.loadConceptHierarchy(engine, subjects.toString(), objects.toString(), gdm);
				logger.debug("Loaded Concept");
				RDFEngineHelper.loadRelationHierarchy(engine, predicates.toString(), gdm);
				logger.debug("Loaded Relation");
				RDFEngineHelper.loadPropertyHierarchy(engine,predicates.toString(), containsRelation, gdm);
			} catch(RuntimeException ex) {
				ex.printStackTrace();
			}
		}
		gdm.genBaseConcepts();
		gdm.genBaseGraph();
		System.out.println("Vert Store : " + gdm.getVertStore().toString());
		System.out.println("Edge Store : " + gdm.getEdgeStore().toString());
	}
	

	public StatementCollector parseQuery(String query) {
		StatementCollector collector = new StatementCollector();
		try {
			SPARQLParser parser = new SPARQLParser();

			System.out.println("Query is " + query);
			ParsedQuery query2 = parser.parseQuery(query, null);
			System.out.println(">>>" + query2.getTupleExpr());
			query2.getTupleExpr().visit(collector);

		} catch (RuntimeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return collector;
	}

	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getRawVar(varName);
	}
	
	private Object addToSPObuffers(String varName, SesameJenaSelectStatement sjss, StringBuffer subjects, StringBuffer predicates, StringBuffer objects, Set<String> subVars, Set<String> predVars, Set<String> objVars){
		Object val = sjss.getRawVar(varName);
		//if it is a uri, brackets need to be put around it. If it is a literal, though, it needs no modification
		String appendString = val + "";
		if(val != null && !(val instanceof Literal || val instanceof com.hp.hpl.jena.rdf.model.Literal))
			appendString = "(<" + appendString +">)";
		else
			return sjss.getVar(varName); // need to get the literal value if it is a literal and don't need it for any hierarchy queries
		
		//store value based on where in the pattern it has appeared
		if(subVars.contains(varName))
			subjects.append(appendString);
		if(predVars.contains(varName))
			predicates.append(appendString);
		if(objVars.contains(varName))
			objects.append(appendString);
		return val;
	}

	@Override
	public Object getData() {
		Hashtable returnHash = (Hashtable) super.getData();
		if(overlay){
			returnHash.put("nodes", gdm.getIncrementalVertStore());
			returnHash.put("edges", gdm.getIncrementalEdgeStore().values());
		}
		else {
			returnHash.put("nodes", gdm.getVertStore());
			returnHash.put("edges", gdm.getEdgeStore().values());
		}
		return returnHash;
	}
}
