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
package prerna.algorithm.impl;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.impl.BigDataEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class ModifyMasterDB {
	protected static final Logger logger = LogManager.getLogger(ModifyMasterDB.class.getName());

	//variables for the master database
	protected static final String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
	protected static final String fileSeparator = System.getProperty("file.separator");
	protected String masterDBName = "MasterDatabase";
	protected BigDataEngine masterEngine;
	
	//uri variables
	protected static final String semossURI = "http://semoss.org/ontologies";
	protected static final String semossConceptURI = semossURI + "/" + Constants.DEFAULT_NODE_CLASS;
	protected static final String semossRelationURI = semossURI + "/" + Constants.DEFAULT_RELATION_CLASS;
	protected static final String resourceURI = "http://www.w3.org/2000/01/rdf-schema#Resource";
	protected static final String mcBaseURI = semossConceptURI+"/MasterConcept";
	protected static final String mccBaseURI = semossConceptURI+"/MasterConceptConnection";
	protected static final String keywordBaseURI = semossConceptURI+"/Keyword";
	protected static final String engineBaseURI = semossConceptURI+"/Engine";
	protected final static String serverBaseURI = semossConceptURI + "/" + "Server";
	protected final static String userBaseURI = semossConceptURI + "/" + "User";
	protected final static String userInsightBaseURI = semossConceptURI + "/" + "UserInsight";
	protected final static String insightBaseURI = semossConceptURI + "/" + "Insight";

	protected static final String engineInsightBaseURI = semossRelationURI + "/Engine:Insight";
	protected static final String enginePerspectiveBaseURI = semossRelationURI + "/Engine:Perspective";
	protected static final String engineKeywordBaseURI = semossRelationURI + "/Has";
	protected static final String engineMCCBaseURI = semossRelationURI + "/Has";	
	protected static final String mcKeywordBaseURI = semossRelationURI + "/ConsistsOf";
	protected static final String mccToMCBaseURI = semossRelationURI + "/To";
	protected static final String mccFromMCBaseURI = semossRelationURI + "/From";
	protected static final String engineServerBaseURI = semossRelationURI + "/HostedOn";
	protected static final String userUserInsightBaseURI = semossRelationURI + "/PartOf";
	protected static final String insightUserInsightBaseURI = semossRelationURI + "/PartOf";

	protected static final String propURI = semossRelationURI + "/" + "Contains";
	protected static final String similarityPropURI = propURI + "/" + "SimilarityScore";
	protected static final String baseURIPropURI = propURI + "/" + "BaseURI";
	protected static final String timesClickedPropURI = propURI + "/" + "TimesClicked";
	
	public ModifyMasterDB() {
		this.masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(masterDBName);
	}
	public ModifyMasterDB(String masterDBName) {
		this.masterDBName = masterDBName;
		this.masterEngine = (BigDataEngine) DIHelper.getInstance().getLocalProp(this.masterDBName);
	}
}
