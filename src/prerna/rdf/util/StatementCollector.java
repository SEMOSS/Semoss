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
package prerna.rdf.util;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;

public class StatementCollector extends QueryModelVisitorBase<Exception> {
	
	private static final Logger logger = LogManager.getLogger(StatementCollector.class.getName());
	
	private List<StatementPattern> statementPatterns = new Vector<StatementPattern>();
	private Set<String> subjectVariables = new HashSet<String>();//keep track of variables that are subjects
	private StringBuffer subjectURIstring = new StringBuffer("");
	private Set<String> predicateVariables = new HashSet<String>();//keep track of variables that are predicates
	private StringBuffer predicateURIstring = new StringBuffer("");
	private Set<String> objectVariables = new HashSet<String>();//keep track of variables that are objects
	private StringBuffer objectURIstring = new StringBuffer("");

	@Override
	public void meet(StatementPattern node) {
		statementPatterns.add(node);
		
		if(node.getSubjectVar().isAnonymous())
			subjectURIstring.append("(<").append(node.getSubjectVar().getValue()).append(">)");
		else
			subjectVariables.add(node.getSubjectVar().getName());
		
		if(node.getPredicateVar().isAnonymous())
			predicateURIstring.append("(<").append(node.getPredicateVar().getValue()).append(">)");
		else
			predicateVariables.add(node.getPredicateVar().getName());
		
		if(node.getObjectVar().isAnonymous())
			objectURIstring.append("(<").append(node.getObjectVar().getValue()).append(">)");
		else
			objectVariables.add(node.getObjectVar().getName());
	}

	public List<StatementPattern> getPatterns() {
		return this.statementPatterns;
	}
	
	public StringBuffer getSubjectURIstring() {
		return subjectURIstring;
	}

	public StringBuffer getPredicateURIstring() {
		return predicateURIstring;
	}

	public Set<String> getSubjectVariables() {
		return subjectVariables;
	}

	public Set<String> getPredicateVariables() {
		return predicateVariables;
	}

	public Set<String> getObjectVariables() {
		return objectVariables;
	}

	public StringBuffer getObjectURIstring() {
		return objectURIstring;
	}
}