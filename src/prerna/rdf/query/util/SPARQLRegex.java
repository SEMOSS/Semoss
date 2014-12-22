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
package prerna.rdf.query.util;

public class SPARQLRegex {
	
	TriplePart var, value;
	Boolean isValueString;
	String regexString;
	
	//TODO: incorporate having clauses inside of a regex

	public SPARQLRegex(TriplePart var, TriplePart value, boolean isValueString)
	{
		if(!var.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql variable");
		}
		if(!value.getType().equals(TriplePart.LITERAL))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql literal");
		}
		this.var = var;
		this.value = value;
		this.isValueString = isValueString;
	}
	
	public void createString()
	{
		String subjectString = SPARQLQueryHelper.createComponentString(var);
		String objectString = SPARQLQueryHelper.createComponentString(value);
		if(!isValueString)
		{
			subjectString = "STR(" + subjectString + ")";
		}
		regexString = "REGEX(" + subjectString + ", " + objectString + " )";
	}
	
	public String getRegexString()
	{
		createString();
		return regexString;
	}
	
	public Object getVar()
	{
		return var;
	}
	
	public Object getValue()
	{
		return value;
	}

}
