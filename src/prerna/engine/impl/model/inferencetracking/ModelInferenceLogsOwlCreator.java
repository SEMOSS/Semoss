/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.engine.impl.model.inferencetracking;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class ModelInferenceLogsOwlCreator {

	// each column name paired to its type in a var
	private List<Pair<String, String>> agentColumns = null;
	private List<Pair<String, String>> roomColumns = null;
	private List<Pair<String, String>> messageColumns = null;
	private List<Pair<String, String>> feedbackColumns = null;
	private List<Pair<String, String>> workspaceColumns = null;
	private List<Pair<String, String>> workspaceResourceColumns = null;
	private List<Pair<String, String>> skillColumns = null;

	// pairs table name with table's primary keys
	private List<Pair<String, Pair<List<String>, List<String>>>> primaryKeys = null;

	// pairs table name with table's foreign keys
	private List<Pair<String, Pair<List<String>, Pair<List<String>, List<String>>>>> foreignKeys = null;
	// Pairs table name with its respective columns
	private List<Pair<String, List<Pair<String, String>>>> allSchemas = null;

	// concepts are tables within db
	// props are cols w/i concepts
	private static List<String> conceptsRequired = new ArrayList<>();
	static {
		conceptsRequired.add("AGENT");
		conceptsRequired.add("ROOM");
		conceptsRequired.add("MESSAGE");
		conceptsRequired.add("FEEDBACK");
		conceptsRequired.add("WORKSPACE");
		conceptsRequired.add("WORKSPACE_RESOURCE");
		conceptsRequired.add("SKILL");
	}

	private IRDBMSEngine modelInferenceDb;

	public ModelInferenceLogsOwlCreator(IRDBMSEngine modelInferenceDb) {
		this.modelInferenceDb = modelInferenceDb;
		createColumnsAndTypes(this.modelInferenceDb.getQueryUtil());
//		definePrimaryKeys();
//		defineForeignKeys(); // TODO make this generic for all rdms engines
	}

	private void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String BLOB_DATATYPE_NAME = queryUtil.getBlobDataTypeName();
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();
		final String INTEGER_DATATYPE_NAME = queryUtil.getIntegerDataTypeName();
		final String DOUBLE_DATATYPE_NAME = queryUtil.getDoubleDataTypeName();

		// @formatter:off
		this.agentColumns = Arrays.asList(
				Pair.with("AGENT_ID", "VARCHAR(50)"),
				Pair.with("AGENT_NAME", "VARCHAR(255)"),
				Pair.with("DESCRIPTION", "VARCHAR(255)"),
				Pair.with("AGENT_TYPE", "VARCHAR(50)"),
				Pair.with("AUTHOR", "VARCHAR(255)"),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME)
			);
		// @formatter:on

//		this.roomColumns = Arrays.asList(
//				Pair.with("INSIGHT_ID", "VARCHAR(50)"),
//				Pair.with("ROOM_NAME", "VARCHAR(255)"),
//				Pair.with("ROOM_CONTEXT", CLOB_DATATYPE_NAME),
//				//Pair.with("ROOM_CONFIG_DATA", CLOB_DATATYPE_NAME),
//				Pair.with("USER_ID", "VARCHAR(255)"),
//				Pair.with("USER_NAME", "VARCHAR(255)"),
//                Pair.with("USER_EMAIL_ID", "VARCHAR(50)"),
//				Pair.with("AGENT_TYPE", "VARCHAR(50)"),
//                Pair.with("AGENT_ID", "VARCHAR(50)"),
//				Pair.with("IS_ACTIVE", BOOLEAN_DATATYPE_NAME),
//				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
//				Pair.with("PROJECT_ID", "VARCHAR(50)"),
//				Pair.with("PROJECT_NAME", "VARCHAR(255)")
//			);

		// @formatter:off
		this.roomColumns = Arrays.asList(
			    Pair.with("INSIGHT_ID", "VARCHAR(50)"),
			    Pair.with("ROOM_ID", "VARCHAR(50)"),
			    Pair.with("ROOM_NAME", "VARCHAR(255)"),
			    Pair.with("ROOM_CONTEXT", CLOB_DATATYPE_NAME),
			    Pair.with("USER_ID", "VARCHAR(255)"),
			    Pair.with("USER_NAME", "VARCHAR(255)"),
			    Pair.with("USER_EMAIL_ID", "VARCHAR(50)"),
			    Pair.with("AGENT_TYPE", "VARCHAR(50)"),
			    Pair.with("AGENT_ID", "VARCHAR(50)"),
			    Pair.with("IS_ACTIVE", BOOLEAN_DATATYPE_NAME),
			    Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
			    Pair.with("UPDATED_AT", TIMESTAMP_DATATYPE_NAME),
			    Pair.with("PROJECT_ID", "VARCHAR(50)"),
			    Pair.with("PROJECT_NAME", "VARCHAR(255)"),
			    Pair.with("MODEL_ID", "VARCHAR(255)"),
			    Pair.with("MESSAGES", CLOB_DATATYPE_NAME),
			    Pair.with("PINNED", BOOLEAN_DATATYPE_NAME),
			    Pair.with("OPTIONS", CLOB_DATATYPE_NAME),
			    Pair.with("SHARE_ID", "VARCHAR(255)"),
			    Pair.with("WORKSPACE_ID", "VARCHAR(255)"),
			    Pair.with("PARENT_ROOM_ID", "VARCHAR(50)")
			);
		// @formatter:on

		// @formatter:off
		this.messageColumns = Arrays.asList(
				Pair.with("MESSAGE_ID", "VARCHAR(50)"),
				Pair.with("TRANSACTION_ID", "VARCHAR(50)"),
				Pair.with("MESSAGE_TYPE", "VARCHAR(50)"),
				Pair.with("MESSAGE_DATA", BLOB_DATATYPE_NAME),
				Pair.with("MESSAGE_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("INPUT_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("OUTPUT_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("THINKING_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("CACHE_READ_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("CACHE_CREATION_TOKENS", INTEGER_DATATYPE_NAME),
				Pair.with("MESSAGE_METHOD", "VARCHAR(50)"),
				//Pair.with("MESSAGE_SEPARATOR", "VARCHAR(50)"),
				Pair.with("RESPONSE_TIME", DOUBLE_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("AGENT_ID", "VARCHAR(50)"),
				Pair.with("MODEL_ID", "VARCHAR(50)"),
				Pair.with("INSIGHT_ID", "VARCHAR(50)"),
			    Pair.with("ROOM_ID", "VARCHAR(50)"),
				Pair.with("SESSIONID", "VARCHAR(255)"),
				Pair.with("USER_ID", "VARCHAR(255)"),
				Pair.with("USER_NAME", "VARCHAR(255)"),
				Pair.with("USER_EMAIL_ID", "VARCHAR(50)")
			);
		// @formatter:on

		// @formatter:off
		this.feedbackColumns = Arrays.asList(
				Pair.with("MESSAGE_ID", "VARCHAR(50)"),
				Pair.with("MESSAGE_TYPE", "VARCHAR(50)"),
				Pair.with("FEEDBACK_TEXT", CLOB_DATATYPE_NAME),
				Pair.with("FEEDBACK_DATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("RATING", BOOLEAN_DATATYPE_NAME)
			);
		// @formatter:on

		// CONFIG_JSON holds the full AgentConfig serialization for this workspace -
		// system_prompt mirror, mcps, budgets, hooks. See AgentConfigLoader for the
		// read order (CONFIG_JSON-first with legacy column/WORKSPACE_RESOURCE fallback)
		// and the workspace setter reactors (SetAgentHooks etc.) for the write path.
		// @formatter:off
		this.workspaceColumns = Arrays.asList(
				Pair.with("WORKSPACE_ID", "VARCHAR(255)"),
				Pair.with("OWNER", "VARCHAR(255)"),
				Pair.with("NAME", "VARCHAR(255)"),
				Pair.with("DESCRIPTION", CLOB_DATATYPE_NAME),
				Pair.with("SYSTEM_PROMPT", CLOB_DATATYPE_NAME),
				Pair.with("CONFIG_JSON", CLOB_DATATYPE_NAME),
				Pair.with("IS_ACTIVE", BOOLEAN_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DATE_UPDATED", TIMESTAMP_DATATYPE_NAME)
			);
		// @formatter:on

		// @formatter:off
		this.workspaceResourceColumns = Arrays.asList(
				Pair.with("WORKSPACE_RESOURCE_ID", "VARCHAR(255)"),
				Pair.with("WORKSPACE_ID", "VARCHAR(255)"),
				Pair.with("RESOURCE_ID", "VARCHAR(255)"),
				Pair.with("RESOURCE_TYPE", "VARCHAR(255)"),
				Pair.with("RESOURCE_SUBTYPE", "VARCHAR(255)")
			);
		// @formatter:on

		// Skill registry. SKILL_ID == the underlying Project ID (skills are projects
		// of type SKILL, identified via PROJECTMETA tag = Skill_Project). The Project
		// owns content, versioning (git in version/), and permissions. This table only
		// holds the skill-specific metadata used for fast listing and the
		// platform-vs-user
		// ORIGIN distinction.
		// @formatter:off
		this.skillColumns = Arrays.asList(
				Pair.with("SKILL_ID", "VARCHAR(50)"),
				Pair.with("SLUG", "VARCHAR(255)"),
				Pair.with("NAME", "VARCHAR(255)"),
				Pair.with("DESCRIPTION", CLOB_DATATYPE_NAME),
				Pair.with("CREATED_BY", "VARCHAR(255)"),
				Pair.with("ORIGIN", "VARCHAR(50)"),
				Pair.with("CONFIG_JSON", CLOB_DATATYPE_NAME),
				Pair.with("DATE_CREATED", TIMESTAMP_DATATYPE_NAME),
				Pair.with("DATE_UPDATED", TIMESTAMP_DATATYPE_NAME)
			);
		// @formatter:on

		// @formatter:off
		this.allSchemas = Arrays.asList(
				Pair.with("AGENT", agentColumns),
				Pair.with("ROOM", roomColumns),
				Pair.with("MESSAGE", messageColumns),
				Pair.with("FEEDBACK", feedbackColumns),
				Pair.with("WORKSPACE", workspaceColumns),
				Pair.with("WORKSPACE_RESOURCE", workspaceResourceColumns),
				Pair.with("SKILL", skillColumns)
			);
		// @formatter:on
	}

//	public void definePrimaryKeys() {
//		// returns ArrayList so its ordered
//		this.primaryKeys = Arrays.asList(
//				Pair.with("AGENT", Pair.with(Arrays.asList("AGENT_ID"), Arrays.asList("VARCHAR(50)"))),
////				Pair.with("ROOM", Pair.with(Arrays.asList("INSIGHT_ID"), Arrays.asList("VARCHAR(50)"))),
//				Pair.with("MESSAGE", Pair.with(Arrays.asList("MESSAGE_ID","MESSAGE_TYPE"), Arrays.asList("VARCHAR(50)","VARCHAR(50)"))),
//				Pair.with("WORKSPACE", Pair.with(Arrays.asList("WORKSPACE_ID"), Arrays.asList("VARCHAR(255)"))),
//				Pair.with("WORKSPACE_RESOURCE", Pair.with(Arrays.asList("WORKSPACE_RESOURCE_ID"), Arrays.asList("VARCHAR(255)")))
//			);
//	}
//	
//	public void defineForeignKeys() {
//		this.foreignKeys = Arrays.asList(
//				// remove this so that UI joins are clean
//				//Pair.with("ROOM", Pair.with(Arrays.asList("AGENT_ID"), Pair.with(Arrays.asList("AGENT"), Arrays.asList("AGENT_ID")))),
//				Pair.with("MESSAGE", Pair.with(Arrays.asList("INSIGHT_ID","AGENT_ID"), Pair.with(Arrays.asList("ROOM","AGENT"), Arrays.asList("INSIGHT_ID","AGENT_ID")))),
//				Pair.with("FEEDBACK", Pair.with(Arrays.asList("MESSAGE_ID,MESSAGE_TYPE"), Pair.with(Arrays.asList("MESSAGE"), Arrays.asList("MESSAGE_ID,MESSAGE_TYPE")))),
//				Pair.with("WORKSPACE_RESOURCE", Pair.with(Arrays.asList("WORKSPACE_ID"), Pair.with(Arrays.asList("WORKSPACE"), Arrays.asList("WORKSPACE_ID"))))
//			);
//	}

	/**
	 * Determine if we need to remake the OWL
	 * 
	 * @return
	 */
	public boolean needsRemake() {
		/*
		 * This is a very simple check Just looking at the tables Not doing anything
		 * with columns but should eventually do that
		 */

		List<String> cleanConcepts = new ArrayList<>();
		List<String> concepts = modelInferenceDb.getPhysicalConcepts();
		for (String concept : concepts) {
			if (concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String cTable = Utility.getInstanceName(concept);
			cleanConcepts.add(cTable);
		}

		if (!cleanConcepts.containsAll(conceptsRequired)) {
			return true;
		}

		// check all columns
		for (Pair<String, List<Pair<String, String>>> tableWithColumns : allSchemas) {
			String tableName = tableWithColumns.getValue0();
			String[] columnNames = tableWithColumns.getValue1().stream().map(Pair::getValue0).toArray(String[]::new);

			for (String columnName : columnNames) {
				if (columnChecks(tableName, columnName)) {
					return true;
				}
			}
		}

		// does not need to be remade
		return false;
	}

	private boolean columnChecks(String tableName, String columnName) {
		String propsURI = "http://semoss.org/ontologies/Concept/" + tableName;
		String relationURI = "http://semoss.org/ontologies/Relation/Contains/" + columnName + "/" + tableName;

		List<String> props = modelInferenceDb.getPropertyUris4PhysicalUri(propsURI);
		if (!props.contains(relationURI)) {
			return true;
		}

		return false;
	}

	/**
	 * Remake the OWL
	 * 
	 * @throws Exception
	 */
	public void remakeOwl() throws Exception {
		try (WriteOWLEngine owlEngine = modelInferenceDb.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.createEmptyOWLFile();
			// write the new OWL
			writeNewOwl(owlEngine);
		}
	}

	/**
	 * Method that uses the OWLER to generate a new OWL structure
	 * 
	 * @param owlLocation
	 * @throws Exception
	 */
	private void writeNewOwl(WriteOWLEngine owler) throws Exception {
		for (Pair<String, List<Pair<String, String>>> columns : allSchemas) {
			String tableName = columns.getValue0();
			owler.addConcept(tableName, null, null);
			for (Pair<String, String> x : columns.getValue1()) {
				owler.addProp(tableName, x.getValue0(), x.getValue1());
			}
		}

		owler.commit();
		owler.export();
	}

	public List<Pair<String, List<Pair<String, String>>>> getDBSchema() {
		return this.allSchemas;
	}

	public List<Pair<String, Pair<List<String>, List<String>>>> getDBPrimaryKeys() {
		return this.primaryKeys;
	}

	public List<Pair<String, Pair<List<String>, Pair<List<String>, List<String>>>>> getDBForeignKeys() {
		return this.foreignKeys;
	}

}
