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
package prerna.nameserver;

import prerna.util.Constants;

public final class MasterDatabaseURIs {

	private MasterDatabaseURIs() {
		
	}
	
	public static final String SEMOSS_URI = "http://semoss.org/ontologies";
	public static final String SEMOSS_CONCEPT_URI = SEMOSS_URI + "/" + Constants.DEFAULT_NODE_CLASS;
	public static final String SEMOSS_RELATION_URI = SEMOSS_URI + "/" + Constants.DEFAULT_RELATION_CLASS;
	public static final String PROP_URI = SEMOSS_RELATION_URI + "/Contains";

	public static final String RESOURCE_URI = "http://www.w3.org/2000/01/rdf-schema#Resource";
	public static final String MC_BASE_URI = SEMOSS_CONCEPT_URI+"/MasterConcept";
	public static final String KEYWORD_BASE_URI = SEMOSS_CONCEPT_URI+"/Keyword";
	public static final String ENGINE_BASE_URI = SEMOSS_CONCEPT_URI+"/Engine";
	
	public static final String USER_BASE_URI = SEMOSS_CONCEPT_URI + "/User";
	public static final String USER_INSIGHT_BASE_URI = SEMOSS_CONCEPT_URI + "/UserInsight";
	public static final String INSIGHT_BASE_URI = SEMOSS_CONCEPT_URI + "/Insight";
	public static final String PERSPECTIVE_BASE_URI = SEMOSS_CONCEPT_URI+"/Perspective";
	public static final String ENGINE_KEYWORD_BASE_URI = SEMOSS_RELATION_URI + "/Has";
}
