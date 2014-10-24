package prerna.ui.components.specific.cbp;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Set;
import java.util.StringTokenizer;

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

import com.bigdata.rdf.model.BigdataLiteralImpl;

import prerna.om.GraphDataModel;
import prerna.om.SEMOSSEdge;
import prerna.om.SEMOSSVertex;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.rdf.util.StatementCollector;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.RDFEngineHelper;
import prerna.ui.components.playsheets.BrowserPlaySheet;
import prerna.ui.main.listener.impl.ColumnChartGroupedStackedListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GBCHeatTablePlaySheet extends BrowserPlaySheet{

	private static final Logger logger = LogManager.getLogger(GBCHeatTablePlaySheet.class.getName());
	
	final String row1Name = "Company Costs ($US M)";
	final String row3Name = "Gap / Advantage ($US M)";
	final String row4Name = "Gap / Advantage as % of total function cost";
	final String salesColName = "http://semoss.org/ontologies/Concept/FunctionCategoryLevel0/Sales";
	final String adjustedLineColName = "Adjusted_Line_of_Business";
	final String TotalLineColName = "Total_Line_of_Business";
	final String valueKey = "valueKey";
	final String rowKey = "rowKey";
	final String colKey = "colKey";
	final String[] colNames = new String[] {"http://semoss.org/ontologies/Concept/FunctionCategoryLevel0/Sales", "http://semoss.org/ontologies/Concept/FunctionLevel1/Marketing", "http://semoss.org/ontologies/Concept/FunctionLevel1/Product", "http://semoss.org/ontologies/Concept/FunctionLevel1/Distribution", "http://semoss.org/ontologies/Concept/FunctionCategoryLevel0/New_Business", "http://semoss.org/ontologies/Concept/FunctionCategoryLevel0/Customer_Service", "http://semoss.org/ontologies/Concept/FunctionCategoryLevel0/Corporate_Overhead", "http://semoss.org/ontologies/Concept/FunctionLevel1/Premium_Tax", "Adjusted_Line_of_Business", "Total_Line_of_Business"};
	String title = "";
	
	ArrayList<String> passedQueries = new ArrayList<String>();
	
	public GBCHeatTablePlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/columnchart.html";
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash = new Hashtable<String, Hashtable<String, Hashtable<String, Object>>>(); // column header -> rowName -> value
		for( String passedQuery : passedQueries)
		{
			processingHash = processQuery(passedQuery, processingHash);
		}
		
		processingHash = addTotalColumns(processingHash);
		
		ArrayList<Hashtable<String, Hashtable<String, Object>>> dataObj = new ArrayList<Hashtable<String, Hashtable<String, Object>>>();
		ArrayList<Hashtable<String, Hashtable<String, Object>>> orderedArray = getOrderedArray(processingHash);
		dataObj.addAll(orderedArray);
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("title", this.title);
		columnChartHash.put("dataSeries", dataObj);
		
		return columnChartHash;
	}
	
	private ArrayList<Hashtable<String, Hashtable<String, Object>>> getOrderedArray(Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash)
	{
		ArrayList<Hashtable<String, Hashtable<String, Object>>> orderedArray = new ArrayList<Hashtable<String, Hashtable<String, Object>>>();
		
		int count = 0;
		for(int colIdx = 0; colIdx < this.colNames.length; colIdx++){
			String colName = this.colNames[colIdx];
			if(processingHash.containsKey(colName)){
				orderedArray.add(count, processingHash.get(colName));
				count++;
			}
		}
		return orderedArray;
	}
	
	private Hashtable<String, Hashtable<String, Hashtable<String, Object>>> addTotalColumns(Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash){		
		//for each column, sum row 1 and 3
		Double val1Total = 0.;
		Double val3Total = 0.;
		Double val1Adj = 0.;
		Double val3Adj = 0.;
		for(String colKey : processingHash.keySet())
		{
			Hashtable<String, Hashtable<String, Object>> colHash = processingHash.get(colKey);
			if(colHash.containsKey(row1Name)){
				//add to row 1 sum
				Double val1 = (Double) colHash.get(row1Name).get(valueKey);
				val1Total = val1Total + val1;
				if(!colKey.equals(this.salesColName)){ // sales is excluded from first total
					val1Adj = val1Adj + val1;
				}
			}
			if(colHash.containsKey(row3Name)){
				//add to row 3 sum
				Double val3 = (Double) colHash.get(row3Name).get(valueKey);
				val3Total = val3Total + val3;
				if(!colKey.equals(this.salesColName)){
					val3Adj = val3Adj + val3;
				}
			}
			
		}
		overrideRow(this.adjustedLineColName, processingHash, val1Adj, row1Name);
		overrideRow(this.adjustedLineColName, processingHash, val3Adj, row3Name);
		overrideRow(this.adjustedLineColName, processingHash, val3Adj / val1Adj, row4Name);
		
		overrideRow(this.TotalLineColName, processingHash, val1Total, row1Name);
		overrideRow(this.TotalLineColName, processingHash, val3Total, row3Name);
		overrideRow(this.TotalLineColName, processingHash, val3Total / val1Total, row4Name);
		
		return processingHash;
	}
	
	private Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processQuery(String passedQuery, Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash)
	{
		SesameJenaSelectWrapper sjsw = Utility.processQuery(this.engine, passedQuery);
		String[] passedNames = sjsw.getVariables();
		while(sjsw.hasNext())
		{
			SesameJenaSelectStatement sjss = sjsw.next();
			
			if(title.isEmpty())
				title = sjss.getVar(passedNames[3]) + " vs. " + sjss.getVar(passedNames[7]);
			
			String colHeader = getVariable(passedNames[1], sjss) + "";
			Double row1Val = ((BigdataLiteralImpl)getVariable(passedNames[4], sjss)).doubleValue();
			overrideRow(colHeader, processingHash, row1Val, row1Name);
			
			if(getVariable(passedNames[6], sjss) != null){
				Double row3Client = ((BigdataLiteralImpl)getVariable(passedNames[6], sjss)).doubleValue();
				Double row3Group = ((BigdataLiteralImpl)getVariable(passedNames[8], sjss)).doubleValue();
				Double row3Den = ((BigdataLiteralImpl)getVariable(passedNames[10], sjss)).doubleValue();
				Double row3Val = (row3Client - row3Group) * row3Den;
				
 				row3Val = addToRow(colHeader, processingHash, row3Val, row3Name);
				
				Double row4Val = row3Val / row1Val;
				overrideRow(colHeader, processingHash, row4Val, row4Name);
			}
			
		}
		
		return processingHash;
	}
	
	private void overrideRow(String colHeader, Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash, Double rowVal, String rowName){
		Hashtable<String, Object> rowHash = new Hashtable<String, Object>();
		Hashtable<String, Hashtable<String, Object>> colHash = new Hashtable<String, Hashtable<String, Object>>();
		
		if(processingHash.containsKey(colHeader))
			colHash = processingHash.get(colHeader);
		else
			processingHash.put(colHeader, colHash);
		
		colHash.put(rowName, rowHash);
		rowHash.put(rowKey, rowName);
		rowHash.put(colKey, colHeader);
		
		rowHash.put(valueKey, rowVal);
		processingHash.put(colHeader, colHash);
	}
	
	private Double addToRow(String colHeader, Hashtable<String, Hashtable<String, Hashtable<String, Object>>> processingHash, Double rowVal, String rowName){
		Hashtable<String, Object> rowHash = new Hashtable<String, Object>();
		Hashtable<String, Hashtable<String, Object>> colHash = new Hashtable<String, Hashtable<String, Object>>();
		if(processingHash.containsKey(colHeader)){
			colHash = processingHash.get(colHeader);
			if(colHash.containsKey(rowName)){
				rowHash = colHash.get(rowName);
				rowVal = rowVal + (Double) rowHash.get(valueKey);
			}
			else {
				colHash.put(rowName, rowHash);
				rowHash.put(rowKey, rowName);
				rowHash.put(colKey, colHeader);
			}
		}
		else{
			colHash.put(rowName, rowHash);
			rowHash.put(rowKey, rowName);
			rowHash.put(colKey, colHeader);
		}
		rowHash.put(valueKey, rowVal);
		processingHash.put(colHeader, colHash);
		
		return rowVal;
	}

	@Override
	public void createData()
	{
		list = new ArrayList<Object[]>();
		list.add(new Object[0]);
		names = new String[0];
		dataHash = processQueryData();
	}

	@Override
	public void setQuery(String query) {
		String [] tokens = query.split("\\+\\+\\+");
		// format is mainQuery+++taxonomyQuery
		for (int queryIdx = 0; queryIdx < tokens.length; queryIdx++){
			String token = tokens[queryIdx];
			passedQueries.add(token);
			logger.info("Set passed query " + token);
		}
	}
	
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getRawVar(varName);
	}
	
}
