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
		Hashtable<String, ArrayList<Hashtable<String, Object>>> dataObj = new Hashtable<String, ArrayList<Hashtable<String, Object>>>();
		//series name - all objects in that series (x : ... , y : ...)
		int lastCol = names.length - 1 ;
		for( int i = 0; i < list.size(); i++)
		{
			Object[] elemValues = list.get(i);
			for( int seriesVal = 1; seriesVal <= elemValues.length / 2; seriesVal++)
			{
				ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
				String seriesName = elemValues[lastCol].toString();
				if(dataObj.containsKey(seriesName))
					seriesArray = dataObj.get(seriesName);
				else
					dataObj.put(seriesName, seriesArray);
				Hashtable<String, Object> elementHash = new Hashtable();
				int firstCol = (seriesVal - 1) * 2;
				elementHash.put("x", elemValues[firstCol].toString());
				elementHash.put("y", elemValues[firstCol+1]);
				elementHash.put("seriesName", elemValues[lastCol].toString());
				seriesArray.add(elementHash);
			}
		}
		
		Hashtable<String, Object> columnChartHash = new Hashtable<String, Object>();
		columnChartHash.put("names", names);
		columnChartHash.put("dataSeries", dataObj.values());
		
		return columnChartHash;
	}
	
}
