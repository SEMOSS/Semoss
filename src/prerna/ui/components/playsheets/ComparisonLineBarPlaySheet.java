package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

public class ComparisonLineBarPlaySheet extends ColumnChartPlaySheet{

	private static final Logger logger = LogManager.getLogger(ComparisonLineBarPlaySheet.class.getName());
	GraphDataModel gdm = new GraphDataModel();
	
	public ComparisonLineBarPlaySheet() 
	{
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/columnchart.html";
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		Hashtable<String, ArrayList<Hashtable<String, Object>>> barObj = new Hashtable<String, ArrayList<Hashtable<String, Object>>>();
		Hashtable<String, ArrayList<Hashtable<String, Object>>> lineObj = new Hashtable<String, ArrayList<Hashtable<String, Object>>>();
		// format of the return will be ?barSeriesName ?yBarVal ?line1SeriesName ?yLine1Val ?line2SeriesName ?yLine2Val ?xValue etc.
		int lastCol = names.length - 1 ;
		ArrayList<String> usedList = new ArrayList<String>();
		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			for( int seriesVal = 1; seriesVal <= elemValues.length / 2; seriesVal++)
			{
				

				int firstCol = (seriesVal - 1) * 2;
				String xVal = elemValues[lastCol].toString();
				String seriesName = elemValues[firstCol].toString();
				
				String usedKey = xVal + seriesName;
				
				if(!usedList.contains(usedKey)){
					usedList.add(usedKey);
					
					// get the right hashtable to fill
					Hashtable<String, ArrayList<Hashtable<String, Object>>> fillHash = lineObj;
					if(seriesVal==1) 
						fillHash = barObj;
					
					
					ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
					
					if(fillHash.containsKey(seriesName))
						seriesArray = fillHash.get(seriesName);
					else
						fillHash.put(seriesName, seriesArray);
					
					Hashtable<String, Object> elementHash = new Hashtable();
					elementHash.put("x",xVal);
					elementHash.put("y", elemValues[firstCol+1]);
					elementHash.put("seriesName",  seriesName);
					seriesArray.add(elementHash);
				}
			}
		}
		Hashtable<String, Collection<ArrayList<Hashtable<String, Object>>>> dataObj = new Hashtable<String, Collection<ArrayList<Hashtable<String, Object>>>>();
		dataObj.put("bar", barObj.values());
		dataObj.put("line", lineObj.values());
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("dataSeries", dataObj);
		
		return columnChartHash;
	}
	
}
