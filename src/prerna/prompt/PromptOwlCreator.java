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
package prerna.prompt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Constants;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class PromptOwlCreator {
	
	// each column name paired to its type in a var
	private List<Pair<String, String>> promptColumns = null;
	private List<Pair<String, String>> promptMetaColumns = null;

	private List<Pair<String, String>> promptMetaKeysColumns = null;

	// Pairs table name with its respective columns
	private List<Pair<String, List<Pair<String, String>>>> allSchemas = null;
	
	// concepts are tables within db
	// props are cols w/i concepts
	private static List<String> conceptsRequired = new ArrayList<>();
	static {
		// prompts
		conceptsRequired.add("PROMPT");
		conceptsRequired.add(Constants.PROMPT_METAKEYS);
		conceptsRequired.add("PROMPTMETA");
	}
	
	private IRDBMSEngine promptsDb;
	
	public PromptOwlCreator(IRDBMSEngine promptEngine) {
		this.promptsDb = promptEngine;
		createColumnsAndTypes(this.promptsDb.getQueryUtil());
	}
	
	private void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();

		this.promptColumns = Arrays.asList(
				Pair.with("ID", "VARCHAR(255)"),
				Pair.with("TITLE", "VARCHAR(255)"),
				Pair.with("CONTEXT", CLOB_DATATYPE_NAME),
				Pair.with("VERSION", "INT"),
				Pair.with("INTENT", "VARCHAR(255)"),
				Pair.with("CREATED_BY", "VARCHAR(255)"),
				Pair.with("DATE_CREATED", "TIMESTAMP"),
				Pair.with("IS_LATEST", BOOLEAN_DATATYPE_NAME),
				Pair.with("GLOBAL", BOOLEAN_DATATYPE_NAME)
			);
		
		this.promptMetaColumns = Arrays.asList(
				Pair.with("PROMPT_ID", "VARCHAR(255)"),
				Pair.with("METAKEY", "VARCHAR(255)"),
				Pair.with("METAVALUE", CLOB_DATATYPE_NAME),
				Pair.with("METAORDER", "INT")
			);
		
		this.promptMetaKeysColumns = Arrays.asList(
				Pair.with("METAKEY", "VARCHAR(255)"),
				Pair.with("SINGLEMULTI", "VARCHAR(255)"),
				Pair.with("DISPLAYORDER", "VARCHAR(255)"),
				Pair.with("DISPLAYOPTIONS", "INT"),
				Pair.with("DEFAULTVALUES", "VARCHAR(255)")
			);
		
		this.allSchemas = Arrays.asList(
				Pair.with("PROMPT", this.promptColumns),
				Pair.with("PROMPTMETA", this.promptMetaColumns),
				Pair.with("PROMPTMETAKEYS", this.promptMetaKeysColumns)				
			);
	}
	
	/**
	 * Determine if we need to remake the OWL
	 * @return
	 */
	public boolean needsRemake() {
		/*
		 * This is a very simple check
		 * Just looking at the tables
		 * Not doing anything with columns but should eventually do that
		 */
		
		List<String> cleanConcepts = new ArrayList<>();
		List<String> concepts = promptsDb.getPhysicalConcepts();
		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
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
			String[] columnNames = tableWithColumns.getValue1().stream()
					.map(Pair::getValue0).toArray(String[]::new);

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
		String relationURI = "http://semoss.org/ontologies/Relation/Contains/" 
				+ columnName + "/" + tableName; 

		List<String> props = promptsDb.getPropertyUris4PhysicalUri(propsURI);	
		if(!props.contains(relationURI)) {
			return true;
		}
		
		return false;
	}
		
	
	/**
	 * Remake the OWL 
	 * @throws Exception 
	 */
	public void remakeOwl() throws Exception {
		try(WriteOWLEngine owlEngine = promptsDb.getOWLEngineFactory().getWriteOWL()) {
			owlEngine.createEmptyOWLFile();
			// write the new OWL
			writeNewOwl(owlEngine);
		}
	}
	
	/**
	 * Method that uses the OWLER to generate a new OWL structure
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
}
