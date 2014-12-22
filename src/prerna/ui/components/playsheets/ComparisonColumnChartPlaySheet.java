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
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Set;
import java.util.Vector;

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
//		int lastCol = names.length - 1 ;
		int seriesCol = 4 ;
		ArrayList<String> usedList = new ArrayList<String>();
		ArrayList<String> seriesList = new ArrayList<String>();
		ArrayList<String> clientIndex = new ArrayList<String>();
		for( int i = 0; i < list.size(); i++)
		{	
			//format is ?x1 ?x1val ?x2 ?x2val .... ?seriesName
			Object[] elemValues = list.get(i);
			String seriesName = elemValues[seriesCol].toString();
			
			// need a way of passing metric ids on the series name.... for now splitting on "+++"
			int splitIdx = seriesName.indexOf("+++");
			String seriesKey = "";
			String seriesUri = "";
			if(splitIdx>=0){
				seriesUri = seriesName.substring(0, splitIdx);
				seriesName = seriesName.substring(splitIdx+3);
				seriesKey = seriesUri;
			}
			else {
				seriesKey = seriesName;
			}

			// get the correct series array and add to dataObj if not already there
			ArrayList<Hashtable<String,Object>> seriesArray = new ArrayList<Hashtable<String,Object>>();
			if(seriesList.contains(seriesKey))
				seriesArray = dataObj.get(seriesList.indexOf(seriesKey));
			else{
				seriesList.add(seriesList.size(), seriesKey);
				dataObj.add(seriesList.indexOf(seriesKey), seriesArray);
			}
			
			//add the element hashtables to the series array -- right now we only allow two. The rest will be properties on that column
			for( int seriesVal = 1; seriesVal <= 2; seriesVal++)
			{
				int firstCol = (seriesVal - 1) * 2;
				
				String xVal = elemValues[firstCol].toString();
				Double yVal = (Double) elemValues[firstCol+1];
				
				String usedKey = xVal + seriesName;
				
				if(!usedList.contains(usedKey)){
					usedList.add(usedKey);
					Hashtable<String, Object> elementHash = new Hashtable();
					elementHash.put("x", xVal);
					elementHash.put("y", yVal);
					elementHash.put("seriesName", seriesName);
					if(!seriesUri.isEmpty())
						elementHash.put("seriesUri", seriesKey);
					
					//figure out where to store it. Want all client values first and then peer group values
					int index = seriesArray.size();
					if(seriesVal == 1)
					{
						elementHash.put("client", true);
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
		if (names.length > 4 && list.size()>0){
			for( int i = 5; i < names.length; i++) {
				String value = list.get(0)[i].toString();
				columnChartHash.put(names[i], value);
			}
		}
		
		columnChartHash.put("names", names);
		columnChartHash.put("dataSeries", dataObj);
		columnChartHash.put("seriesList", seriesList);
		
		return columnChartHash;
	}
	
}
