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
package prerna.ui.components.playsheets;

import java.awt.Dimension;
import java.util.Hashtable;

import org.openrdf.model.Literal;

import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * The Play Sheet for creating a heat map diagram.  
 */
public class HeatMapPlaySheet extends BrowserPlaySheet {
	
	/**
	 * Constructor for HeatMapPlaySheet.
	 */
	public HeatMapPlaySheet() {
		super();
		this.setPreferredSize(new Dimension(800,600));
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		fileName = "file://" + workingDir + "/html/MHS-RDFSemossCharts/app/heatmap.html";
	}
	
	/**
	 * Method processQueryData.  Processes the data from the SPARQL query into an appropriate format for the specific play sheet.
	
	 * @return Hashtable - Consists of the x-value, y-value, x- and y-axis titles, and the title of the map.*/
	public Hashtable processQueryData()
	{
		Hashtable dataHash = new Hashtable();
		Hashtable dataSeries = new Hashtable();
		String[] var = wrapper.getVariables();
		String xName = var[0];
		String yName = var[1];
		for (int i=0;i<list.size();i++)
		{
			Hashtable elementHash = new Hashtable();
			Object[] listElement = list.get(i);			
			String methodName = listElement[0].toString();
			String groupName = listElement[1].toString();
			String key = methodName +"-"+groupName;
			double count = (Double) listElement[2];
			elementHash.put(xName, methodName);
			elementHash.put(yName, groupName);
			elementHash.put(var[2], count);
			dataHash.put(key, elementHash);
			
		}

		Hashtable allHash = new Hashtable();
		allHash.put("dataSeries", dataHash);
		String[] var1 = wrapper.getVariables();
		allHash.put("title",  var1[0] + " vs " + var1[1]);
		allHash.put("xAxisTitle", var1[0]);
		allHash.put("yAxisTitle", var1[1]);
		allHash.put("value", var1[2]);
		
		return allHash;
	}
	
	@Override
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		Object var = sjss.getRawVar(varName);
			if( var != null && var instanceof Literal) {
				var = sjss.getVar(varName);
			} 
		return var;
	}
}
