/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.util;

import java.util.ArrayList;

/**
 * Enables a variable to be a set of predefined constants.
 * This class defines constants for all types of playsheets, which includes their names, source file location, and the hint for a SPARQL query associated with each.
 */

public enum PlaySheetEnum {
	
	Graph("Graph", "prerna.ui.components.playsheets.GraphPlaySheet", "GraphPlaySheet Hint: CONSTRUCT {?subject ?predicate ?object} WHERE{ ... }"),
	Grid("Grid", "prerna.ui.components.playsheets.GridPlaySheet", "GridPlaySheet Hint: SELECT ?x1 ?x2 ?x3 WHERE{ ... }"),
	Raw_Grid("Raw Grid", "prerna.ui.components.playsheets.GridRAWPlaySheet", "GridRAWPlaySheet Hint: SELECT ?x1 ?x2 ?x3 WHERE{ ... }"),
	Heat_Map("Heat Map", "prerna.ui.components.playsheets.HeatMapPlaySheet", "HeatMapPlaySheet Hint: SELECT ?xAxisList ?yAxisList ?numericHeatValue WHERE{ ... } GROUP BY ?xAxisList ?yAxisList"),
	Parallel_Coordinates("Parallel Coordinates", "prerna.ui.components.playsheets.ParallelCoordinatesPlaySheet", "ParallelCoordinatesPlaySheet Hint: SELECT ?axis1 ?axis2 ?axis3 WHERE{ ... }"),
	Grid_Scatter("Grid Scatter", "prerna.ui.components.playsheets.ScatterChartPlaySheet", "GridScatterSheet Hint: SELECT ?elementName ?xAxisValues ?yAxisValues (OPTIONAL)?zAxisValues WHERE{ ... }"),
	Column_Chart("Column Chart", "prerna.ui.components.playsheets.ColumnChartPlaySheet", "ColumnChartPlaySheet Hint: SELECT ?xAxis ?yAxis1 (OPTIONAL) ?yAxis2 ?yAxis3 ... (where all yAxis values are numbers) WHERE { ... }"),
	Binned_Column_Chart("Binned Column Chart", "prerna.ui.components.playsheets.BinnedColumnChartPlaySheet", "BinnedColumnChartPlaySheet Hint: SELECT ?id ?value (where id is a unique identifier and value is its given value) WHERE { ... }"),
	Pie_Chart("Pie Chart", "prerna.ui.components.playsheets.PieChartPlaySheet", "PieChartPlaySheet Hint: SELECT ?wedgeName ?wedgeValue WHERE { ... }"),
	Binned_Pie_Chart("Binned Pie Chart", "prerna.ui.components.playsheets.BinnedPieChartPlaySheet", "BinnedPieChartPlaySheet Hint: SELECT ?id ?value (where id is a unique identifier and value is its given value) WHERE { ... }"),
	DualEngine("Dual Engine", "prerna.ui.components.playsheets.DualEngineGenericPlaySheet", "DualEngineGenericPlaySheet Hint: psName&engine1&engine2&query1&query2&outer1&outer2"),
	
	Machine_Learning_Module("Machine Learning Module", "prerna.ui.components.playsheets.MachineLearningModulePlaySheet", "MachineLearningModulePlaySheet Hint: SELECT ?instance ?categoricalProp ?numericalProp WHERE { ... }"),
	Clustering("Clustering", "prerna.ui.components.playsheets.ClusteringVizPlaySheet", "ClusteringVizPlaySheet Hint: SELECT ?instance ?categoricalProp ?numericalProp WHERE { ... }+++(OPTIONAL)numClusters"),
	Local_Outlier_Factor("Outlier","prerna.ui.components.playsheets.OutlierVizPlaySheet", "OutlierPlaySheet Hint: SELECT ?instance ?prop1... ?propN WHERE{ ... }+++NeighborhoodCount"),
	Classification("Classifier","prerna.ui.components.playsheets.WekaClassificationPlaySheet", "WekaClassificationPlaySheet Hint: SELECT ?instance ?prop1... ?propN ?classificationProp WHERE{ ... }+++ClassificationAlgorithm"),
	DatasetSimilarity("Similarity", "prerna.ui.components.playsheets.DatasetSimilarityPlaySheet", "DatasetSimilarityPlaySheet Hint: SELECT ?instance ?prop1... ?propN WHERE{ ... }"),
	DatasetSimilarityColumnChart("Similarity Column Chart", "prerna.ui.components.playsheets.DatasetSimilairtyColumnChartPlaySheet", "DatasetSimilairtyColumnChartPlaySheet Hint: SELECT ?instance ?prop1... ?propN WHERE{ ... }"),
	
	Sankey_Diagram("Sankey Diagram","prerna.ui.components.playsheets.SankeyPlaySheet", "SankeyPlaySheet Hint: SELECT ?source ?target ?value ?target2 ?value2 ?target3 ?value3...etc  Note: ?target is the source for ?target2 and ?target2 is the source for ?target3...etc WHERE{ ... }"),
	World_Map("World Map","prerna.ui.components.playsheets.OCONUSMapPlaySheet", "OCONUSMapPlaySheet Hint: SELECT ?label ?latitude ?longitude WHERE{ ... }"),
	World_Heat_Map("World Heat Map","prerna.ui.components.playsheets.WorldHeatMapPlaySheet", "WorldHeatMapPlaySheet Hint: SELECT ?country ?numericHeatValue WHERE{ ... }"),
	US_Heat_Map("US Heat Map","prerna.ui.components.playsheets.USHeatMapPlaySheet", "USHeatMapPlaySheet Hint: SELECT ?state ?numericHeatValue WHERE{ ... }"),
	Circle_Pack("Circle Pack","prerna.ui.components.playsheets.CirclePackPlaySheet", "CirclePackPlaySheet Hint: SELECT ?level1 ?level2 ... ?size WHERE{ ... }"),
	Parallel_Sets("Parallel Sets","prerna.ui.components.playsheets.ParallelSetsPlaySheet", "ParallelSetsPlaySheet Hint: SELECT ?axis1 ?axis2 ?axis3 WHERE{ ... }"),
	Dendrogram("Dendrogram","prerna.ui.components.playsheets.DendrogramPlaySheet", "DendrogramPlaySheet Hint: SELECT ?level1 ?level2 ?level3 WHERE{ ... }"),
	LineChart("Line Chart", "prerna.ui.components.playsheets.LineChartPlaySheet", "LineChartPlaySheet Hint: SELECT ?xAxis ?series1 (OPTIONAL) ?series2 ?series3 ... (where all series values are numbers) WHERE { ... }");
	
	private final String sheetName;
	private final String sheetClass;
	private final String sheetHint;
	
	PlaySheetEnum(String playSheetName, String playSheetClass, String playSheetHint) {
		this.sheetName = playSheetName;
		this.sheetClass = playSheetClass;
		this.sheetHint = playSheetHint;
	}
	
	public String getSheetClass(){
		return this.sheetClass;
	}
	
	public String getSheetName(){
		return this.sheetName;
	}
	
	public String getSheetHint(){
		return this.sheetHint;
	}
	
	public static ArrayList<String> getAllSheetNames(){
		ArrayList<String> list = new ArrayList<String>();
		for (PlaySheetEnum e : PlaySheetEnum.values())
		{
			list.add(e.getSheetName());
		}
		return list;
	}
	
	public static ArrayList<String> getAllSheetClasses(){
		ArrayList<String> list = new ArrayList<String>();
		for (PlaySheetEnum e : PlaySheetEnum.values())
		{
			list.add(e.getSheetClass());
		}
		return list;
	}
	
	public static String getClassFromName(String sheetName){
		String match = "";
		for(PlaySheetEnum e : PlaySheetEnum.values())
			if(e.getSheetName().equals(sheetName))
			{
				match = e.getSheetClass();
			}
		return match;
	}
	
	public static String getHintFromName(String sheetName){
		String match = "";
		for(PlaySheetEnum e : PlaySheetEnum.values())
			if(e.getSheetName().equals(sheetName))
			{
				match = e.getSheetHint();
			}
		return match;
	}
	
	public static String getNameFromClass(String sheetClass){
		String match = "";
		for(PlaySheetEnum e : PlaySheetEnum.values())
			if(e.getSheetClass().equals(sheetClass))
			{
				match = e.getSheetName();
			}
		return match;
	}
	
	public static PlaySheetEnum getEnumFromClass(String sheetClass){
		PlaySheetEnum match = null;
		for(PlaySheetEnum e : PlaySheetEnum.values())
			if(e.getSheetClass().equals(sheetClass))
			{
				match = e;
			}
		return match;		
	}
}
