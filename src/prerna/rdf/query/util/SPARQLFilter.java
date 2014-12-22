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

import java.util.ArrayList;

public class SPARQLFilter {

	ArrayList<SPARQLRegex> regexList = new ArrayList<SPARQLRegex>();
	Boolean or;
	String filterString;
	
	
	//TODO: figure out logic to have both and and or logic in filter
	//TODO: add clauses within filter
	
	public SPARQLFilter(ArrayList<Object> filterArr, boolean or)
	{
		for(Object filterElem : filterArr)
		{
			if(filterElem.getClass().equals(SPARQLRegex.class))
			{
				regexList.add((SPARQLRegex) filterElem);
			}
			// this is where you add addition if statements to build out other lists of objects to place in filter
			else {
				throw new IllegalArgumentException("Filter cannot be used with expressions of type " + filterElem.getClass().toString());
			}
		}
		this.or = or;
	}
	
	public void createString()
	{
		filterString = "FILTER( ";
		// loop through all the different objects that can be passed into Filter
		for(int i = 0; i < regexList.size(); i++)
		{
			if(or && i > 0)
			{
				filterString += " || " + regexList.get(i).getRegexString();
			} else if(!or && i > 0) {
				filterString += " && " + regexList.get(i).getRegexString();
			} else {
				filterString += regexList.get(i).getRegexString();
			}
		}
		// this is where you add addition loops for other types of objects to place in filter
		filterString += " )";
	}
	
	public String getFilterString() {
		createString();
		return filterString;
	}
}
