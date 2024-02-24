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
package prerna.rdf.engine.wrappers;

import java.io.IOException;
import java.util.List;

import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;
import prerna.util.Constants;
import prerna.util.Utility;

public class JenaSelectWrapper extends AbstractWrapper implements ISelectWrapper {
	
	private static final Logger LOGGER = LogManager.getLogger(JenaSelectWrapper.class.getName());
	
	transient ResultSet rs = null;
	
	@Override
	public boolean hasNext() {
		return rs.hasNext();
	}

	@Override
	public ISelectStatement next() 
	{
		ISelectStatement thisSt = new SelectStatement();
	    QuerySolution row = rs.nextSolution();
		for(int colIndex = 0;colIndex < headers.length;colIndex++)
		{
			String value = row.get(rawHeaders[colIndex])+"";
			RDFNode node = row.get(rawHeaders[colIndex]);
			
			thisSt.setVar(headers[colIndex], getRealValue(node));
			thisSt.setRawVar(headers[colIndex], value);
			LOGGER.debug("Binding Name " + rawHeaders[colIndex]);
			LOGGER.debug("Binding Value " + value);
		}	
		return thisSt;
	}

	@Override
	public String[] getVariables() {
		return getDisplayVariables();
	}
	
	@Override
	public String[] getDisplayVariables() {
		headers = new String[rs.getResultVars().size()];
		List <String> names = rs.getResultVars();
		for (int colIndex = 0; colIndex < names.size(); colIndex++){
			String columnLabel = names.get(colIndex);
			String tableLabel = names.get(colIndex);
			boolean columnIsProperty = false;
			String tableLabelURI = Constants.CONCEPT_URI;
			String columnLabelURI = Constants.PROPERTY_URI;
			if(columnLabel.contains("__")){
				columnIsProperty = true;
				String[] splitColAndTable = columnLabel.split("__");
				tableLabel = splitColAndTable[0];
				columnLabel = splitColAndTable[1];
			}
		
			tableLabelURI += tableLabel;
			columnLabelURI += columnLabel;
			//now get the display name 
			tableLabel = Utility.getInstanceName(tableLabelURI);
			columnLabel = Utility.getInstanceName(columnLabelURI);
			if(columnIsProperty){
				columnLabel = tableLabel + "__" + columnLabel;
			} else {
				columnLabel = tableLabel;
			}
			
			headers[colIndex] = columnLabel;
		}
		return headers;
	}
	
	@Override
	public String[] getPhysicalVariables() {
		// get the result set metadata to get the column names
		rawHeaders = new String[rs.getResultVars().size()];
		List <String> names = rs.getResultVars();
		for(int colIndex = 0;colIndex < names.size();rawHeaders[colIndex] = names.get(colIndex), colIndex++);
		return rawHeaders;
	}

	@Override
	public void execute() throws Exception {
		rs = (ResultSet) engine.execQuery(query);		
	}
	
	private Object getRealValue(RDFNode node){
		if(node.isAnon())
		{
			LOGGER.debug("Ok.. an anon node");
			return Utility.getNextID();
		}
		else
		{
			LOGGER.debug("Raw data JENA For Column ");
			return Utility.getInstanceName(node + "");
		}
	}

	@Override
	public void close() throws IOException {
		
	}
}
