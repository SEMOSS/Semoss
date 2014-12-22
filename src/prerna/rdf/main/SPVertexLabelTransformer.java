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
