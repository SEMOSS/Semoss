/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
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

import java.util.ArrayList;

public class SPARQLBindings {
	ArrayList<TriplePart> bindSubject;
	TriplePart bindObject;
	String bindingsString;
	
	public SPARQLBindings(ArrayList<TriplePart> bindSubject, TriplePart bindObject)
	{
		if(!bindObject.getType().equals(TriplePart.VARIABLE))
		{
			throw new IllegalArgumentException("Bind object has to be a sparql variable");
		}
		this.bindSubject = bindSubject;
		this.bindObject = bindObject;
	}
	
	public void createString()
	{
		String subjectString = "";
		for(TriplePart subjectPart : bindSubject)
		{
			subjectString = subjectString + "(" + SPARQLQueryHelper.createComponentString(subjectPart) + ")";
		}
		String objectString = SPARQLQueryHelper.createComponentString(bindObject);
		bindingsString = "BINDINGS " + objectString + " {" + subjectString + "}";
	}
	
	public String getBindingString()
	{
		createString();
		return bindingsString;
	}
	
	public Object getBindingSubject()
	{
		return bindSubject;
	}
	
	public Object getBindingObject()
	{
		return bindObject;
	}

}
