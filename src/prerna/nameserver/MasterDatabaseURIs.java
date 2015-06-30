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

public class MasterDatabaseURIs {

	public MasterDatabaseURIs() {
		
	}
	
	public static final String SEMOSS_URI = "http://semoss.org/ontologies";
	public static final String SEMOSS_CONCEPT_URI = SEMOSS_URI + "/" + Constants.DEFAULT_NODE_CLASS;
	public static final String SEMOSS_RELATION_URI = SEMOSS_URI + "/" + Constants.DEFAULT_RELATION_CLASS;
	public static final String PROP_URI = SEMOSS_RELATION_URI + "/Contains";

	public static final String RESOURCE_URI = "http://www.w3.org/2000/01/rdf-schema#Resource";
	public static final String MC_BASE_URI = SEMOSS_CONCEPT_URI+"/MasterConcept";
	public static final String KEYWORD_BASE_URI = SEMOSS_CONCEPT_URI+"/Keyword";
	public static final String PARAM_BASE_URI = SEMOSS_CONCEPT_URI+"/Param";
	public static final String ENGINE_RELATION_BASE_URI = SEMOSS_CONCEPT_URI+"/EngineRelation";
	public static final String ENGINE_BASE_URI = SEMOSS_CONCEPT_URI+"/Engine";
	
	public static final String USER_BASE_URI = SEMOSS_CONCEPT_URI + "/User";
	public static final String USER_INSIGHT_BASE_URI = SEMOSS_CONCEPT_URI + "/UserInsight";
	public static final String INSIGHT_BASE_URI = SEMOSS_CONCEPT_URI + "/Insight";
	public static final String PERSPECTIVE_BASE_URI = SEMOSS_CONCEPT_URI+"/Perspective";
	public static final String ENGINE_KEYWORD_BASE_URI = SEMOSS_RELATION_URI + "/Has";
	
	//User-based URI vars
	public static final String ENGINEROLEGROUP_URI = SEMOSS_CONCEPT_URI+"/EngineRoleGroup";
	public static final String USERGROUP_URI = SEMOSS_CONCEPT_URI+"/UserGroup";
	public static final String ROLE_URI = SEMOSS_CONCEPT_URI+"/Role";
	public static final String CREATE_PERMISSION_PROP_URI = PROP_URI + "/CreatePermission";
	public static final String READ_PERMISSION_PROP_URI = PROP_URI + "/ReadPermission";
	public static final String MODIFY_PERMISSION_PROP_URI = PROP_URI + "/ModifyPermission";
	public static final String ENGINE_ROLEGROUP_REL_URI = SEMOSS_RELATION_URI + "/HasRoleGroup";
	public static final String ENGINEROLEGROUP_ROLE_REL_URI = SEMOSS_RELATION_URI + "/IsRole";
	public static final String ROLEGROUP_USERGROUP_REL_URI = SEMOSS_RELATION_URI + "/HasUserGroup";
	public static final String USERGROUP_USER_REL_URI = SEMOSS_RELATION_URI + "/HasUser";
	public static final String USER_NAME_PROP_URI = PROP_URI + "/Name";
	public static final String USER_EMAIL_PROP_URI = PROP_URI + "/Email";
	public static final String EXPLORE_PERMISSION_PROP_URI = "/ExplorePermission";
	public static final String TRAVERSE_PERMISSION_PROP_URI = "/TraversePermission";
	public static final String CREATE_INSIGHT_PERMISSION_PROP_URI = "/CreateInsightPermission";
	public static final String COPY_INSIGHT_PERMISSION_PROP_URI = "/CopyInsightPermission";
	public static final String EDIT_INSIGHT_PERMISSION_PROP_URI = "/EditInsightPermission";
	public static final String DELETE_INSIGHT_PERMISSION_PROP_URI = "/DeleteInsightPermission";
	public static final String MODIFY_DATA_PERMISSION_PROP_URI = "/ModifyDataPermission";
	
	//Engine access request URIs
	public static final String ENGINE_ACCESSREQUEST_URI = SEMOSS_CONCEPT_URI+"/EngineAccessRequest";
	public static final String USER_ENGINE_ACCESSREQUEST_REL_URI = SEMOSS_RELATION_URI + "/HasRequest";
	public static final String ENGINE_NAME_REQUESTED_PROP_URI = PROP_URI + "/EngineNameRequested";
	public static final String ENGINE_ACCESS_REQUESTOR_PROP_URI = PROP_URI + "/EngineAccessRequestor";
	public static final String ENGINE_ACCESS_PERMISSIONS_PROP_URI = PROP_URI + "/EngineAccessPermissions";
	
	//User activity tracking URIs
	public static final String USERINSIGHT_URI = SEMOSS_CONCEPT_URI + "/UserInsight";
	public static final String USERINSIGHT_EXECUTION_COUNT_PROP_URI = PROP_URI + "/ExecutionCount";
	public static final String USERINSIGHT_LAST_EXECUTED_DATE_PROP_URI = PROP_URI + "/LastExecutedDate";
	public static final String USERINSIGHT_PUBLISH_VISIBILITY_PROP_URI = PROP_URI + "/PublishVisibility";
	public static final String USERINSIGHT_FAVORITED_PROP_URI = PROP_URI + "/Favorited";
	public static final String USER_USERINSIGHT_REL_URI = SEMOSS_RELATION_URI + "/RunsInsight";
	public static final String INSIGHT_USERINSIGHT_REL_URI = SEMOSS_RELATION_URI + "/ExecutedBy";
}
