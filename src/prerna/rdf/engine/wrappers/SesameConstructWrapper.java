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

import org.openrdf.model.Statement;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryEvaluationException;

import prerna.rdf.engine.api.IConstructStatement;
import prerna.rdf.engine.api.IConstructWrapper;
import prerna.rdf.engine.impl.SesameJenaConstructStatement;

public class SesameConstructWrapper extends AbstractWrapper implements
		IConstructWrapper {

	public transient GraphQueryResult gqr = null;
	
	@Override
	public IConstructStatement next() {
		IConstructStatement thisSt = new ConstructStatement();
		try {
			logger.debug("Adding a sesame statement ");
			Statement stmt = gqr.next();
			thisSt.setSubject(stmt.getSubject()+"");
			thisSt.setObject(stmt.getObject());
			thisSt.setPredicate(stmt.getPredicate() + "");
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return thisSt;
	}

	@Override
	public void execute() {
		gqr = (GraphQueryResult)engine.execGraphQuery(this.query);
	}

	@Override
	public boolean hasNext() {
		boolean retBool = false;
		
		try {
			retBool = gqr.hasNext();
			if(!retBool)
				gqr.close();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return retBool;
	}
}
