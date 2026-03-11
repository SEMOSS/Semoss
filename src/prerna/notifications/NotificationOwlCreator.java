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
package prerna.notifications;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.javatuples.Pair;

import prerna.engine.api.IRDBMSEngine;
import prerna.engine.impl.owl.WriteOWLEngine;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class NotificationOwlCreator {

	// each column name paired to its type in a var
	private List<Pair<String, String>> notificationColumns = null;

	// Pairs table name with its respective columns
	private List<Pair<String, List<Pair<String, String>>>> allSchemas = null;

	// concepts are tables within db
	// props are cols w/i concepts
	private static List<String> conceptsRequired = new ArrayList<>();
	static {
		// notification
		conceptsRequired.add("NOTIFICATION");
	}

	private IRDBMSEngine notificationDb;

	public NotificationOwlCreator(IRDBMSEngine notificationEngine) {
		this.notificationDb = notificationEngine;
		createColumnsAndTypes(this.notificationDb.getQueryUtil());
	}

	private void createColumnsAndTypes(AbstractSqlQueryUtil queryUtil) {
		final String CLOB_DATATYPE_NAME = queryUtil.getClobDataTypeName();
		final String BOOLEAN_DATATYPE_NAME = queryUtil.getBooleanDataTypeName();
		final String TIMESTAMP_DATATYPE_NAME = queryUtil.getDateWithTimeDataType();

		this.notificationColumns = Arrays.asList(Pair.with("NOTIFICATIONID", "VARCHAR(255)"),
				Pair.with("RECIPIENTID", "VARCHAR(255)"), Pair.with("RECIPIENTTYPE", "VARCHAR(255)"),
				Pair.with("NOTIFICATIONTITLE", "VARCHAR(255)"), Pair.with("MESSAGE", CLOB_DATATYPE_NAME),
				Pair.with("ACTIONTYPE", "VARCHAR(50)"), Pair.with("ACTIONTARGET", "VARCHAR(255)"),
				Pair.with("ISREAD", BOOLEAN_DATATYPE_NAME), Pair.with("PRIORITY", "VARCHAR(20)"),
				Pair.with("NOTIFICATIONTYPE", "VARCHAR(255)"), Pair.with("CATALOGID", "VARCHAR(255)"),
				Pair.with("CREATEDBY", "VARCHAR(255)"), Pair.with("CREATEDDATE", TIMESTAMP_DATATYPE_NAME),
				Pair.with("READDATE", TIMESTAMP_DATATYPE_NAME), Pair.with("NOTIFICATIONSOURCE", "VARCHAR(255)"),
				Pair.with("USERID", "VARCHAR(255)"), Pair.with("USERTYPE", "VARCHAR(255)"),
				Pair.with("USEREXISTINGROLE", "VARCHAR(255)"), Pair.with("USERNEWROLE", "VARCHAR(255)"));

		this.allSchemas = Arrays.asList(Pair.with("NOTIFICATION", this.notificationColumns));
	}

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
		List<String> concepts = notificationDb.getPhysicalConcepts();
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

		List<String> props = notificationDb.getPropertyUris4PhysicalUri(propsURI);
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
		try (WriteOWLEngine owlEngine = notificationDb.getOWLEngineFactory().getWriteOWL()) {
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
}
