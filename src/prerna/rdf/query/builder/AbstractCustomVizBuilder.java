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
package prerna.rdf.query.builder;

import java.util.Hashtable;

import prerna.rdf.query.util.SEMOSSQuery;

public abstract class AbstractCustomVizBuilder implements ICustomVizBuilder{
	public final static String vizTypeKey = "visualizationType";
	SEMOSSQuery semossQuery = new SEMOSSQuery();

	public Hashtable<String, Object> allJSONHash = new Hashtable<String, Object>();
	
	public String visualType = "";
	@Override
	public void setVisualType(String visualType) {
		this.visualType = visualType;
	}

	@Override
	public String getVisualType() {
		return visualType;
	}

	@Override
	public void setJSONDataHash(Hashtable<String, Object> allJSONHash) {
		this.allJSONHash = allJSONHash;
	}

	@Override
	public Hashtable<String, Object> getJSONDataHash() {
		return allJSONHash;
	}

	@Override
	public String getQuery() {
		return semossQuery.getQuery();
	}
	
	public SEMOSSQuery getSEMOSSQuery(){
		return this.semossQuery;
	}

	@Override
	public void buildQuery() {
		
	}
}
