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
package prerna.rdf.main;

import org.apache.commons.collections15.Transformer;

import prerna.om.DBCMVertex;
import prerna.util.Constants;


/**
 */
public class SPVertexLabelTransformer implements Transformer<DBCMVertex, String> {

	/**
	 * Method transform.
	 * @param arg0 DBCMVertex
	
	 * @return String */
	@Override
	public String transform(DBCMVertex arg0) {
		return (String)arg0.getProperty(Constants.VERTEX_NAME);
	}

}
