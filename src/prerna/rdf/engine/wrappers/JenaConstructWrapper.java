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
package prerna.rdf.engine.wrappers;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IConstructWrapper;
import prerna.util.Utility;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.StmtIterator;

public class JenaConstructWrapper extends AbstractWrapper implements IConstructWrapper {
	
	transient Model model = null;
	transient StmtIterator si = null;
	

	@Override
	public IConstructStatement next() {
		IConstructStatement thisSt = new ConstructStatement();

		com.hp.hpl.jena.rdf.model.Statement stmt = si.next();
		logger.debug("Adding a JENA statement ");
		Resource sub = stmt.getSubject();
		Property pred = stmt.getPredicate();
		RDFNode node = stmt.getObject();
		if(node.isAnon())
			thisSt.setPredicate(Utility.getNextID());
		else 	
			thisSt.setPredicate(stmt.getPredicate() + "");

		if(sub.isAnon())
			thisSt.setSubject(Utility.getNextID());
		else
			thisSt.setSubject(stmt.getSubject()+"");
		
		if(node.isAnon())
			thisSt.setObject(Utility.getNextID());
		else
			thisSt.setObject(stmt.getObject());
		
		return thisSt;
	}

	@Override
	public void execute() {
		model = (Model)engine.execQuery(query);
		si = model.listStatements();
	}

	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return si.hasNext();
	}

}
