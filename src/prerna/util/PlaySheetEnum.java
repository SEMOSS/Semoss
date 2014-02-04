/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.util;

import java.util.ArrayList;

/**
 * Enables a variable to be a set of predefined constants.
 * This class defines constants for all types of playsheets, which includes their names, source file location, and the hint for a SPARQL query associated with each.
 */

public enum PlaySheetEnum {
	
	Graph("Graph", "prerna.ui.components.playsheets.GraphPlaySheet", "Hint: CONSTRUCT {?subject ?predicate ?object} WHERE{ ... }"),
	Grid("Grid", "prerna.ui.components.playsheets.GridPlaySheet", "Hint: SELECT ?x1 ?x2 ?x3 WHERE{ ... }"),
	Raw_Grid("Raw Grid", "prerna.ui.components.playsheets.GridRAWPlaySheet", "Hint: SELECT ?x1 ?x2 ?x3 WHERE{ ... }"),
	Heat_Map("Heat Map", "prerna.ui.components.playsheets.HeatMapPlaySheet", "Hint: SELECT ?xAxisList ?yAxisList ?numericHeatValue WHERE{ ... } GROUP BY ?xAxisList ?yAxisList"),
	Parallel_Coordinates("Parallel Coordinates", "prerna.ui.components.playsheets.ParallelCoordinatesPlaySheet", "Hint: SELECT ?axis1 ?axis2 ?axis3 WHERE{ ...}"),
	Grid_Scatter("Grid Scatter", "prerna.ui.components.playsheets.GridScatterSheet", "Hint: SELECT ?elementName ?xAxisValues ?yAxisValues (OPTIONAL)?zAxisValues WHERE{ ... }"),
	Sankey_Diagram("Sankey Diagram","prerna.ui.components.playsheets.SankeyPlaySheet", "Hint: SELECT ?source ?target ?value ?target2 ?value2 ?target3 ?value3...etc  Note: ?target is the source for ?target2 and ?target2 is the source for ?target3...etc WHERE{ ...}"),
	World_Heat_Map("World Heat Map","prerna.ui.components.playsheets.WorldHeatMapPlaySheet", "Hint: SELECT ?country ?numericHeatValue WHERE{ ... }"),
	US_Heat_Map("US Heat Map","prerna.ui.components.playsheets.USHeatMapPlaySheet", "Hint: SELECT ?state ?numericHeatValue WHERE{ ... }"),
	Dendrogram("Dendrogram","veera.ui.components.playsheets.DendrogramPlaySheet", "Hint: SELECT ?x1 ?x2 ?x3 WHERE{ ... }");

	
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
		PlaySheetEnum match = null; //need to initialize as non-null value
		for(PlaySheetEnum e : PlaySheetEnum.values())
			if(e.getSheetClass().equals(sheetClass))
			{
				match = e;
			}
		return match;		
	}
}
