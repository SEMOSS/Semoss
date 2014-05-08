package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Set;

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
import prerna.ui.components.RDFEngineHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ColumnChartPlaySheet extends BrowserPlaySheet{

	GraphDataModel gdm = new GraphDataModel();
	
	public ColumnChartPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/columnchart.html";
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		Hashtable<String, ArrayList<Object>> data = new Hashtable<String, ArrayList<Object>>();
		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			ArrayList<Object> values = new ArrayList<Object>();
			for( int j = 1; j < elemValues.length; j++)
			{
				values.add(elemValues[j]);
			}
			data.put(elemValues[0].toString(), values);	
		}
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("type", "column");
		columnChartHash.put("dataSeries", data);
		
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
			catch (Exception e)
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
		} catch (Exception e) {
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
		boolean loadHierarchy = !(subjects.equals("") && predicates.equals("") && objects.equals("")); 
		if(loadHierarchy) {
			try {
				RDFEngineHelper.loadConceptHierarchy(engine, subjects.toString(), objects.toString(), gdm);
				logger.debug("Loaded Concept");
				RDFEngineHelper.loadRelationHierarchy(engine, predicates.toString(), gdm);
				logger.debug("Loaded Relation");
				RDFEngineHelper.loadPropertyHierarchy(engine,predicates.toString(), containsRelation, gdm);
			} catch(Exception ex) {
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
