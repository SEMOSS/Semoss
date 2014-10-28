package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.util.ArrayList;
import java.util.Arrays;
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

public class ComparisonColumnChartPlaySheet extends ColumnChartPlaySheet{

	private static final Logger logger = LogManager.getLogger(ComparisonColumnChartPlaySheet.class.getName());
	GraphDataModel gdm = new GraphDataModel();
	
	public ComparisonColumnChartPlaySheet() 
	{
		super();
	}
	
	public Hashtable<String, Object> processQueryData()
	{		
		ArrayList<ArrayList<Hashtable<String, Object>>> dataObj = new ArrayList<ArrayList<Hashtable<String, Object>>>();
		//series name - all objects in that series (x : ... , y : ...)
		int lastCol = names.length - 1 ;
		ArrayList<String> usedList = new ArrayList<String>();
		ArrayList<String> seriesList = new ArrayList<String>();
		ArrayList<String> clientIndex = new ArrayList<String>();
		for( int i = 0; i < list.size(); i++)
		{
			//format is ?x1 ?x1val ?x2 ?x2val .... ?seriesName
			Object[] elemValues = list.get(i);
			String seriesName = elemValues[lastCol].toString();
			for( int seriesVal = 1; seriesVal <= elemValues.length / 2; seriesVal++)
			{
				int firstCol = (seriesVal - 1) * 2;
				
				String xVal = elemValues[firstCol].toString();
				Double yVal = (Double) elemValues[firstCol+1];
				
				String usedKey = xVal + seriesName;
				
				if(!usedList.contains(usedKey)){
					usedList.add(usedKey);
					ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
					if(seriesList.contains(seriesName))
						seriesArray = dataObj.get(seriesList.indexOf(seriesName));
					else{
						seriesList.add(seriesList.size(), seriesName);
						dataObj.add(seriesList.indexOf(seriesName), seriesArray);
					}
					Hashtable<String, Object> elementHash = new Hashtable();
					elementHash.put("x", xVal);
					elementHash.put("y", yVal);
					// need a way of passing metric ids on the series name.... for now splitting on "+++"
					int splitIdx = seriesName.indexOf("+++");
					if(splitIdx>=0){
						elementHash.put("seriesName", seriesName.substring(splitIdx+3));
						elementHash.put("seriesUri", seriesName.substring(0, splitIdx));
					}
					else {
						elementHash.put("seriesName", seriesName);
					}
					
					//figure out where to store it. Want all client values first and then peer group values
					int index = seriesArray.size();
					if(seriesVal == 1)
					{
						if(clientIndex.contains(xVal))
							index = clientIndex.indexOf(xVal);
						else{
							index = clientIndex.size();
							clientIndex.add(index, xVal);
						}
					}
					if(index>seriesArray.size())
						index = seriesArray.size();
					seriesArray.add(index, elementHash);
				}
			}
		}
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("dataSeries", dataObj);
		
		return columnChartHash;
	}
	
}
