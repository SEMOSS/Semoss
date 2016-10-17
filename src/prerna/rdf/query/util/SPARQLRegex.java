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
package prerna.rdf.query.util;

public class SPARQLRegex implements ISPARQLFilterInput{
	
	FILTER_TYPE type = FILTER_TYPE.REGEX_MATCH;
	TriplePart var, value;
	Boolean isValueString;
	String regexString;
	Boolean isCaseSensitive;
	
	//TODO: incorporate having clauses inside of a regex
	public SPARQLRegex(TriplePart var, TriplePart value, boolean isValueString, boolean isCaseSensitive)
	{
		if(!var.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql variable");
		}
		if(value.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql literal or uri");
		}
		this.var = var;
		this.value = value;
		this.isValueString = isValueString;
		this.isCaseSensitive = isCaseSensitive;
	}
	
	public void createString()
	{
		String subjectString = SPARQLQueryHelper.createComponentString(var);
		String objectString = SPARQLQueryHelper.createComponentString(value);
		String caseSensitivityClause = "";
		if(!isValueString) {
			subjectString = "STR(" + subjectString + ")";
		}
		if(!isCaseSensitive){
			caseSensitivityClause = ", 'i' ";
		}
		regexString = "REGEX(" + subjectString + ", " + objectString + " " + caseSensitivityClause +" )";
	}
	
	@Override
	public Object getVar()
	{
		return var;
	}
	
	@Override
	public Object getValue()
	{
		return value;
	}

	@Override
	public String getFilterInput() {
		createString();
		return regexString;
	}

}
