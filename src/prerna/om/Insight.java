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
package prerna.om;

import java.util.Hashtable;

public class Insight extends Hashtable {
	

	String labelKey = "label";	// Label of the question
	
	Hashtable<String, String> propHash = new Hashtable<String, String>();
	String idKey = "id";	// ID of the question
	String engineKey = "engine";	// Engine question is associated with
	String outputKey = "output";	// Output type of the question
	String sparqlKey = "sparql";	// Sparql of the question
	String descrKey = "description";	// Sparql of the question
	String orderKey = "order";	// order of the question

	// database id where this insight is
	// this may be a URL
	// in memory
	// or a file
	String databaseIDkey = "databaseID";
	
	public Insight(){
		this.put("propHash", propHash);
	}
	
	public String getId() {
		return this.propHash.get(this.idKey);
	}
	public void setId(String id) {
		this.propHash.put(this.idKey, id);
	}
	
	public String getOutput() {
		return this.propHash.get(this.outputKey);
	}

	public void setOutput(String output) {
		this.propHash.put(this.outputKey, output);
	}

	public String getLabel() {
		return (String) this.get(this.labelKey);
	}

	public void setLabel(String label) {
		this.put(this.labelKey, label);
	}

	public String getSparql() {
		return this.propHash.get(this.sparqlKey);
	}

	public void setSparql(String sparql) {
		this.propHash.put(this.sparqlKey, sparql);
	}

	public String getEngine() {
		return this.propHash.get(this.engineKey);
	}

	public void setEngine(String engine) {
		this.propHash.put(this.engineKey, engine);
	}

	public String getDescription() {
		return this.propHash.get(this.descrKey);
	}

	public void setDescription(String descr) {
		this.propHash.put(this.descrKey, descr);
	}

	public String getDatabaseID() {
		return this.propHash.get(this.databaseIDkey);
	}

	public void setDatabaseID(String databaseID) {
		this.propHash.put(this.databaseIDkey, databaseID);
	}
	
	public void setOrder (String order) {
		this.propHash.put(this.orderKey, order);
	}

	public String getOrder() {
		return this.propHash.get(this.orderKey);
	}
	
	// type of database where it is
	public enum DB_TYPE {MEMORY, FILE, REST};
	
	
}
