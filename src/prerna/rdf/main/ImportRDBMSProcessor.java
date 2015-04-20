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
package prerna.rdf.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.DataValidation;
import org.apache.poi.ss.usermodel.DataValidationConstraint;
import org.apache.poi.ss.usermodel.Name;
import org.apache.poi.ss.util.CellRangeAddressList;
import org.apache.poi.xssf.usermodel.XSSFDataValidationHelper;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.poi.main.PropFileWriter;
import prerna.ui.components.ImportDataProcessor;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ImportRDBMSProcessor {
	static final Logger logger = LogManager.getLogger(ImportRDBMSProcessor.class.getName());
	
	private StringBuilder tableMapping = new StringBuilder();
	private StringBuilder propertyTypeMapping = new StringBuilder();
	private StringBuilder relationshipTypeMapping = new StringBuilder();
	private StringBuilder relationshipMapping = new StringBuilder();
	private String dbConnection = new String();
	
	public PropFileWriter propWriter;
	
	private String customBaseURI = "";
	private String baseRelURI = "";
	private final String semossURI = "http://semoss.org/ontologies" + "/" + Constants.DEFAULT_NODE_CLASS + "/";
	private final String semossRelURI = "http://semoss.org/ontologies" + "/" + Constants.DEFAULT_RELATION_CLASS + "/";
	private final String propURI = "http://semoss.org/ontologies/" + Constants.DEFAULT_RELATION_CLASS + "/Contains/";
	private String filePath = "";
	private String dbName = "";
	private String type = "";
	private String url = "";
	private String username = "";
	private char[] password;
	
	private String owlPath = "";
	
	private final static String spacer = " \n\t";
	private final int EXCEL_WORKSHEET_NAME_CHARACTER_LIMIT = 30;

	private Set<String> propertyList = new HashSet<String>();
	private Set<String> relationshipList = new HashSet<String>();
	private Set<String> baseConcepts = new HashSet<String>();
	private Set<String> baseRels = new HashSet<String>();
	private Hashtable<String, ArrayList<String>> baseRelationships = new Hashtable<String, ArrayList<String>>();
	
	private final String MYSQL = "MySQL";
	private final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
	private final String ORACLE = "Oracle";
	private final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private final String SQLSERVER = "MS SQL Server";
	private final String SQLSERVER_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
	private final String ASTER = "Aster Database";
	private final String ASTER_DRIVER = "com.asterdata.ncluster.jdbc.core.NClusterJDBCDriver";
	
	public ImportRDBMSProcessor() {
		
	}
	
	public ImportRDBMSProcessor(String customBaseURI, String fileNames, String repoName, String type, String url, String username, char[] password) {
		this.customBaseURI = customBaseURI + "/" + Constants.DEFAULT_NODE_CLASS + "/";
		this.baseRelURI = customBaseURI + "/" + Constants.DEFAULT_RELATION_CLASS + "/";
		this.filePath = fileNames;
		this.dbName = repoName;
		this.type = type;
		// Base DB URL on whether or not a schema/owner is specified in Oracle connection string
		if(type.equals(ORACLE) && url.contains("-")) {
			this.url = url.substring(0, url.indexOf("-"));
		} else {
			this.url = url;
		}
		this.username = username;
		this.password = password;
	}
	
	public boolean setUpRDBMS() throws FileReaderException, EngineException
	{
		if(!checkConnection(this.type, url, username, password)) {
			return false;
		}
		processExcel(this.filePath);
		
		//Change path for where the template file is
		String templatePath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/rdbms/MappingTemplate.ttl";
		String requiredMapping = readRequiredMappings(templatePath);
		
		// Write the file
		String outputDir = "db/" + this.dbName;
		File mappingFileDir = new File(outputDir);
		try {
			if(!mappingFileDir.getCanonicalFile().isDirectory()) {
				if(!mappingFileDir.mkdirs())
					return false;
			}
		} catch(IOException e) {
			e.printStackTrace();
			return false;
		}
		
		File mappingFile = new File(outputDir + "/" + this.dbName + "_Mapping.ttl");
		try{
			FileWriter writer = new FileWriter(mappingFile.getAbsolutePath());
			writer.write("@prefix map: <#> . \n");
			writer.write("@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> . \n");
			writer.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . \n");
			writer.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . \n");
			writer.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . \n");
			writer.write("@prefix jdbc: <http://d2rq.org/terms/jdbc/> . \n");
			writer.write("\n");
			writer.write(createDatabase(this.url, this.username, new String(this.password)));
			writer.write(tableMapping.toString() + "\n" + 
					propertyTypeMapping.toString() + "\n" + 
					relationshipMapping.toString() + "\n" + 
					relationshipTypeMapping.toString() + "\n" + 
					requiredMapping);
			writer.close();
		} catch (IOException e) {
			Utility.showError("Could not create mapping file!");
			e.printStackTrace();
		}
		
		this.owlPath = outputDir + "/" + this.dbName + "_OWL.OWL";
		
		writeOWL(baseConcepts, baseRels, baseRelationships, owlPath);
		writeSMSS(outputDir);
		
		return true;
	}
	
	private void processExcel(String wb){
		String[] files = wb.split(";");
		for(String file : files) {
			XSSFWorkbook workbook = null;
			try {
				workbook = new XSSFWorkbook(new FileInputStream(file));
			} catch (Exception e) {
				e.printStackTrace();
				Utility.showError("Couldn't Find Workbook");
			}

			// process properties
			XSSFSheet propSheet = workbook.getSheet("Nodes");

			// check rows in correct order
			XSSFRow headerPropRow = propSheet.getRow(0);
			if(!headerPropRow.getCell(0).toString().equals("Table") && !headerPropRow.getCell(0).toString().equals("Subject") && !headerPropRow.getCell(0).toString().equals("Property") && !headerPropRow.getCell(0).toString().equals("DataType")){
				logger.error("Headers are incorrect in property sheet! \nPlease correct your workbook format");
			}

			String tableInput = "";
			String tableInstanceColumn = "";
			String propertyName = "";
			String dataType = "";
			String nodeType = "";

			int propRows = propSheet.getLastRowNum();
			for(int i = 1; i <= propRows; i++){
				XSSFRow dataRow = propSheet.getRow(i);
				tableInput = dataRow.getCell(0).toString();
				
				if(tableInput.isEmpty()) {
					continue;
				}
				
				tableInstanceColumn = dataRow.getCell(1).toString();
				
				if(dataRow.getCell(2) != null)
				{
					propertyName = dataRow.getCell(2).toString();
					dataType = dataRow.getCell(3).toString();
					
					if(dataType.equalsIgnoreCase("int") || dataType.equalsIgnoreCase("Integer"))
					{
						dataType = "integer";
					}
					else if(dataType.equalsIgnoreCase("varchar") || dataType.equalsIgnoreCase("String"))
					{
						dataType = "string";
					}
					else if(dataType.equalsIgnoreCase("DateTime") || dataType.equalsIgnoreCase("Date"))
					{
						dataType = "dateTime";
					}
					else if(dataType.equalsIgnoreCase("Double") || dataType.equalsIgnoreCase("Decimal"))
					{
						dataType = "double";
					}
					else if(dataType.equalsIgnoreCase("Float"))
					{
						dataType = "float";
					} else {
						dataType = "string";
					}
				}
				
				nodeType = dataRow.getCell(4).toString();

				if(!baseConcepts.contains(nodeType)) {
					baseConcepts.add(nodeType);
					processTable(tableInput, tableInstanceColumn, nodeType);
				}
				
				if(propertyName != null && !propertyName.equals("") && dataType != null && !dataType.equals(""))
				{
					processTableProperty(tableInput, propertyName, dataType);	
				}
			}

			processProperties(propertyList);

			// process relationships
			XSSFSheet relSheet = workbook.getSheet("Relationships");

			//TODO: add check that columns are in correct order
			String relTable = "";
			String relSubjectColumn = "";
			String relObjectColumn = "";
			String subjectTable = "";
			String objectTable = "";
			String relation = "";
			String subjectInstance = "";
			String objectInstance = "";
			String subjectID = "";
			String objectID = "";
			String subjectNodeType = "";
			String objectNodeType = "";


			int relRows = relSheet.getLastRowNum();
			for(int i = 1; i <= relRows; i++){
				XSSFRow dataRow = relSheet.getRow(i);
				relTable = dataRow.getCell(0).toString();
				relSubjectColumn = dataRow.getCell(1).toString();
				relObjectColumn = dataRow.getCell(2).toString();
				subjectTable = dataRow.getCell(3).toString();
				subjectInstance = dataRow.getCell(4).toString();
				subjectID = dataRow.getCell(5).toString();

				objectTable = dataRow.getCell(6).toString();
				objectInstance = dataRow.getCell(7).toString();
				objectID = dataRow.getCell(8).toString();

				relation = dataRow.getCell(9).toString();
				subjectNodeType = dataRow.getCell(10).toString();
				objectNodeType = dataRow.getCell(11).toString();
				baseConcepts.add(subjectNodeType);
				baseConcepts.add(objectNodeType);
				baseRels.add(relation);
				ArrayList<String> baseRel = new ArrayList<String>();
				baseRel.add(subjectNodeType);
				baseRel.add(relation);
				baseRel.add(objectNodeType);
				baseRelationships.put(String.valueOf(i), baseRel);

				processRelationships(relTable, relSubjectColumn, relObjectColumn, subjectTable, objectTable, relation,
						subjectInstance, objectInstance, subjectID, objectID, subjectNodeType, objectNodeType);
			}

			processRelationshipType(relationshipList);
		}
	}

	private String createDatabase(String url, String username, String password)
	{
		//Account for the single backslash in a SQL Server URL and escape it when writing D2RQ mapping
		if(this.type.equalsIgnoreCase(this.SQLSERVER)) {
			url = url.replace("\\", "\\\\");
		}
		
		String dbConnection = "map:database a d2rq:Database;" + spacer + 
				"d2rq:jdbcDSN \"" + url + "\";" + spacer + 
				"d2rq:jdbcDriver \"";
		
		if(this.type.equalsIgnoreCase(this.MYSQL)) {
			dbConnection += this.MYSQL_DRIVER;
		} else if(this.type.equalsIgnoreCase(this.ORACLE)) {
			dbConnection += this.ORACLE_DRIVER;
		} else if(this.type.equalsIgnoreCase(this.SQLSERVER)) {
			dbConnection += this.SQLSERVER_DRIVER;
		} else if(this.type.equalsIgnoreCase(this.ASTER)) {
			dbConnection += this.ASTER_DRIVER;
		}
		
		dbConnection += "\";" + spacer + 
				"d2rq:username \"" + username + "\";" + spacer + 
				"d2rq:password \"" + password + "\";" + spacer + 
				"jdbc:keepAlive \"3600\";" + spacer + 
				".\n";
		
		return dbConnection;
	}

	private void processTable(String tableName, String tableInstance, String nodeType){
		tableMapping.append("#####Table ").append(tableName).append("\n").append(
				"#Create the instanceNode typeOf baseNode triple \n").append(
				"map:Instance").append(tableName).append("_TypeOf_Base").append(tableName).append(" a d2rq:ClassMap;").append(spacer).append(
				"d2rq:dataStorage map:database;").append(spacer).append(
				"d2rq:uriPattern \"").append(customBaseURI).append(nodeType).append("/@@").append(tableName).append(".").append(tableInstance).append("@@\";").append(spacer).append(
				"d2rq:class ").append("<").append(semossURI).append(nodeType).append(">;").append(spacer).append(
				"d2rq:additionalProperty map:TypeOf_Concept;").append(spacer).append(
				"d2rq:additionalProperty map:SubClassOf_Resource; ").append(spacer).append(
				".\n").append(
				"#Create the baseNode subclassOf Concept triple \n").append(
				"map:Base").append(tableName).append("_SubClassOf_Concept a d2rq:ClassMap;").append(spacer).append(
				"d2rq:dataStorage map:database;").append(spacer).append(
				"d2rq:constantValue <").append(semossURI).append(nodeType).append(">;").append(spacer).append(
				"d2rq:additionalProperty map:SubClassOf_Concept;").append(spacer).append(
				"d2rq:additionalProperty map:SubClassOf_Resource; ").append(spacer).append(
				".\n").append(
				"#####Property Label for Table ").append(tableName).append("\n").append(
				"#Create the rdfs:label for the concept").append(tableName).append(spacer).append(
				"map:Instance").append(tableName).append("_Label a d2rq:PropertyBridge;").append(spacer).append(
				"d2rq:belongsToClassMap map:Instance").append(tableName).append("_TypeOf_Base").append(tableName).append(";").append(spacer).append(
				"d2rq:property rdfs:label;").append(spacer).append(
				"d2rq:column \"").append(tableName).append(".").append(tableInstance).append("\";").append(spacer).append(
				".\n");
	}

	private void processTableProperty(String tableName, String propertyName, String dataType){
		//add property to total list of unique properties
		propertyList.add(propertyName);
		tableMapping.append("#####Property ").append(propertyName).append(" for Table ").append(tableName).append("\n").append(
				"#Create the instanceNode contains/prop propValue triple \n").append(
				"map:Instance").append(tableName).append("_BaseProp_").append(propertyName).append(" a d2rq:PropertyBridge; \n").append(
				"d2rq:belongsToClassMap map:Instance").append(tableName).append("_TypeOf_Base").append(tableName).append(";").append(spacer).append(
				"d2rq:property ").append("<").append(propURI).append(propertyName).append(">;").append(spacer).append(
				"d2rq:column ").append("\"").append(tableName).append(".").append(propertyName).append("\";").append(spacer).append(
				"d2rq:datatype xsd:").append(dataType).append(";").append(spacer).append(
				".\n");
	}
	
	private void processProperties(Set<String> propertyList){
		Iterator<String> propIterator = propertyList.iterator();
		while(propIterator.hasNext()){
			String propertyName = propIterator.next();
			propertyTypeMapping.append("#####Property ").append(propertyName).append("\n").append(
				"#Create the Necessary Definitions for the property ").append(propertyName).append("\n").append(
				"map:Base").append(propertyName).append(" a d2rq:ClassMap;").append(spacer).append(
				"d2rq:dataStorage map:database;").append(spacer).append(
				"d2rq:constantValue <").append(propURI).append(propertyName).append(">;").append(spacer).append(
				"d2rq:additionalProperty map:TypeOf_Property;").append(spacer).append(
				"d2rq:additionalProperty map:TypeOf_Contains;").append(spacer).append(
				"d2rq:additionalProperty map:Base").append(propertyName).append("_SubPropertyOf_Base").append(propertyName).append(";").append(spacer).append(
				".\n").append(
				"map:Base").append(propertyName).append("_SubPropertyOf_Base").append(propertyName).append(" a d2rq:AdditionalProperty;").append(spacer).append(
				"d2rq:propertyName rdfs:subPropertyOf;").append(spacer).append(
				"d2rq:propertyValue <").append(propURI).append(propertyName).append(">;").append(spacer).append(
				".\n");
		}
	}
	
	private void processRelationships(String relTable, String relSubjectColumn, String relObjectColumn, String subjectTable, 
			String objectTable, String relation, String subjectInstance, String objectInstance, String subjectID, String objectID,
			String subjectNodeType, String objectNodeType){
		//add relationship to total list of unique relationships
		relationshipList.add(relation);
		relationshipMapping.append("#####Defining Relationship: ").append(subjectTable).append(" ").append(relation).append(" ").append(objectTable).append("\n").append(
			"#Create the instance ").append(subjectTable).append(" ").append(relation).append(" ").append(objectTable).append("\n").append(
			"map:Instance").append(subjectTable).append("_InstanceRel_Instance").append(objectTable).append(" a d2rq:PropertyBridge;").append(spacer).append(
			"d2rq:belongsToClassMap map:Instance").append(subjectTable).append("_TypeOf_Base").append(subjectTable).append(";").append(spacer).append(
			"d2rq:refersToClassMap map:Instance").append(objectTable).append("_TypeOf_Base").append(objectTable).append(";").append(spacer).append(
			"d2rq:dynamicProperty \"").append(baseRelURI).append(relation).append("/@@").append(subjectTable).append(".").append(subjectInstance).append("@@:@@").append(objectTable).append(".").append(objectInstance).append("@@\";").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relSubjectColumn).append(" = ").append(subjectTable).append( ".").append(subjectID).append("\";").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relObjectColumn).append(" = ").append(objectTable).append( ".").append(objectID).append("\";").append(spacer).append(
			".\n").append(
			"#Create the higher level triples for the relationship \n").append(
			"map:InstanceRel_").append(subjectTable).append("_").append(objectTable).append(" a d2rq:ClassMap;").append(spacer).append(
			"d2rq:dataStorage map:database;").append(spacer).append(
			"d2rq:uriPattern \"").append(baseRelURI).append(relation).append("/@@").append(subjectTable).append(".").append(subjectInstance).append("@@:@@").append(objectTable).append(".").append(objectInstance).append("@@\";").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relSubjectColumn).append(" = ").append(subjectTable).append( ".").append(subjectID).append("\";").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relObjectColumn).append(" = ").append(objectTable).append( ".").append(objectID).append("\";").append(spacer).append(
			"d2rq:additionalProperty map:TypeOf_Property;").append(spacer).append(
			"d2rq:additionalProperty map:SubPropertyOf_Relation;").append(spacer).append(
			"d2rq:additionalProperty map:SubPropertyOf_").append(relation).append("_").append(subjectTable).append("_").append(objectTable).append(";").append(spacer).append(
			".\n").append(
			"map:SubPropertyOf_").append(relation).append("_").append(subjectTable).append("_").append(objectTable).append(" a d2rq:AdditionalProperty;").append(spacer).append(
			"d2rq:propertyName rdfs:subPropertyOf;").append(spacer).append(
			"d2rq:propertyValue <").append(semossRelURI).append(relation).append(">;").append(spacer).append(
			".\n").append(
			"map:Label_").append(relation).append("_").append(subjectTable).append("_").append(objectTable).append(" a d2rq:PropertyBridge;").append(spacer).append(
			"d2rq:belongsToClassMap map:InstanceRel_").append(subjectTable).append("_").append(objectTable).append(";").append(spacer).append(
			"d2rq:property rdfs:label;").append(spacer).append(
			"d2rq:pattern \"@@").append(subjectTable).append(".").append(subjectInstance).append("@@:@@").append(objectTable).append(".").append(objectInstance).append( "@@\";").append(spacer).append(
			".\n").append(
			"map:").append(subjectTable).append("_").append(objectTable).append("_SubPropertyOf_Self a d2rq:PropertyBridge;").append(spacer).append(
			"d2rq:belongsToClassMap map:InstanceRel_").append(subjectTable).append("_").append(objectTable).append(";").append(spacer).append(
			"d2rq:property rdfs:subPropertyOf;").append(spacer).append(
			"d2rq:uriPattern \"").append(baseRelURI).append(relation).append("/@@").append(subjectTable).append(".").append(subjectInstance).append("@@:@@").append(objectTable).append(".").append(objectInstance).append("@@\";").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relSubjectColumn).append(" = ").append(subjectTable).append(".").append(subjectID).append("\";").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relObjectColumn).append(" = ").append(objectTable).append(".").append(objectID).append("\";").append(spacer).append(
			".\n").append(
			"map:Instance").append(subjectTable).append("_Rel_Instance_").append(objectTable).append(" a d2rq:PropertyBridge;").append(spacer).append(
			"d2rq:belongsToClassMap map:Instance").append(subjectTable).append("_TypeOf_Base").append(subjectTable).append(";").append(spacer).append(
			"d2rq:refersToClassMap map:Instance").append(objectTable).append("_TypeOf_Base").append(objectTable).append(";").append(spacer).append(
			"d2rq:property <http://semoss.org/ontologies/Relation>;").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relSubjectColumn).append(" = ").append(subjectTable).append(".").append(subjectID).append("\";").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relObjectColumn).append(" = ").append(objectTable).append(".").append(objectID).append("\";").append(spacer).append(
			".\n").append(
			"map:Instance").append(subjectTable).append("_Rel_").append(objectTable).append(" a d2rq:PropertyBridge;").append(spacer).append(
			"d2rq:belongsToClassMap map:Instance").append(subjectTable).append("_TypeOf_Base").append(subjectTable).append(";").append(spacer).append(
			"d2rq:refersToClassMap map:Instance").append(objectTable).append("_TypeOf_Base").append(objectTable).append(";").append(spacer).append(
			"d2rq:property <").append(semossRelURI).append(relation).append(">;").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relSubjectColumn).append(" = ").append(subjectTable).append(".").append(subjectID).append("\";").append(spacer).append(
			"d2rq:join \"").append(relTable).append(".").append(relObjectColumn).append(" = ").append(objectTable).append(".").append(objectID).append("\";").append(spacer).append(
			".\n");
	}
	
	
	private void processRelationshipType(Set<String> relationshipList){
		Iterator<String> relIterator = relationshipList.iterator();
		while(relIterator.hasNext()){
			String relationshipName = relIterator.next();
			relationshipTypeMapping.append("#####Relationship ").append(relationshipName).append("\n").append(
				"#Create the rel/relName subPropertyOf rel triple \n").append(
				"map:").append(relationshipName).append(" a d2rq:ClassMap;").append(spacer).append(
				"d2rq:dataStorage map:database;").append(spacer).append(
				"d2rq:constantValue <").append(semossRelURI).append(relationshipName).append(">;").append(spacer).append(
				"d2rq:additionalProperty map:SubPropertyOf_Relation;").append(spacer).append(
				".\n");
		}
	}
	
	private String readRequiredMappings(String templatePath){
		String requiredMapping = "";
		try {
			requiredMapping = new Scanner(new File(templatePath)).useDelimiter("\\Z").next();
		} catch (FileNotFoundException e) {
			Utility.showError("Could not find template file!");
			e.printStackTrace();
		}
		return requiredMapping;
	}
	
	private void writeOWL(Set<String> baseConcepts, Set<String> baseRels, Hashtable<String, ArrayList<String>> baseRelationships, String path)
	{
		StringBuilder owlFile = new StringBuilder();
		owlFile.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n").append(
				"<rdf:RDF").append(spacer).append(
					"xmlns=\"http://semoss.org/ontologies/Relation\"").append(spacer).append(
					"xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"").append(spacer).append(
					"xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"> \n").append(
				"<rdfs:Class rdf:about=\"http://semoss.org/ontologies/Concept\"/> \n").append(
				"<rdf:Property rdf:about=\"http://semoss.org/ontologies/Relation\"/> \n");
		
		for(String conceptName : baseConcepts)
		{
			owlFile.append("<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/").append(conceptName).append("\">").append(spacer).append(
				"rdfs:subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept\"/> \n").append(
					"</rdf:Description>\n");
		}
		
//		for(String relName : baseRels)
//		{
//			owlFile.append("<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/").append(relName).append("\">").append(spacer).append(
//				"<rdfs:subPropertyOf rdf:resource=\"http://semoss.org/ontologies/Relation\"/> \n").append(
//					"</rdf:Description>\n");
//		}
		
		for(String key: baseRelationships.keySet())
		{
			ArrayList<String> relation = baseRelationships.get(key);
			String sub = relation.get(0);
			String rel = relation.get(1);
			String obj = relation.get(2);
			
			owlFile.append("<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/").append(sub).append("\">").append(spacer).append(
					"<").append(rel).append(" rdf:resource=\"http://semoss.org/ontologies/Concept/").append(obj).append("\"/> \n").append(
					"</rdf:Description>\n");
		}
		
		owlFile.append("</rdf:RDF>");
		
		try{
			FileWriter owlWriter = new FileWriter(path);
			owlWriter.write(owlFile.toString());
			owlWriter.close();
		} catch (IOException e) {
			Utility.showError("Could not create owl file!");
			e.printStackTrace();
		}
	}
	
	private void writeSMSS(String dbDir) throws FileReaderException, EngineException {	
		propWriter = new PropFileWriter();
		propWriter.setBaseDir(DIHelper.getInstance().getProperty(Constants.BASE_FOLDER));
		propWriter.defaultEngine = "prerna.rdf.engine.impl.RDBMSD2RQEngine";
		propWriter.ontologyFileName = dbDir + "/" + this.dbName + "_Custom_Map.prop";
		propWriter.owlFile = this.owlPath;
		propWriter.hasMap = true;
		propWriter.runWriter(this.dbName, "", "", "", ImportDataProcessor.DB_TYPE.RDF);
	}
	
	public boolean checkConnection(String type, String url, String username, char[] password) {
		boolean isValid = false;
		
		if(!url.contains("jdbc") || url.contains("<") || url.contains(">") || url.contains("[") || url.contains("]")) {
			return isValid;
		}
		
		Connection con;
		
		if(type.equals(this.MYSQL)) {
			try {
				Class.forName(this.MYSQL_DRIVER);
				
				//Connection URL format: jdbc:mysql://<hostname>[:port]/<DBname>?user=username&password=pw
				con = DriverManager
						.getConnection(url + "?user=" + username + "&password=" + new String(password));
				if(con.isValid(10)) {
					isValid = true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		} else if(type.equals(this.ORACLE)) {
			try {
				Class.forName(this.ORACLE_DRIVER);
				
				//Connection URL format: jdbc:oracle:thin:@<hostname>[:port]/<service or sid>
				if(url.contains("-")) {
					url = url.substring(0, url.indexOf("-"));
				}
				con = DriverManager
						.getConnection(url, username, new String(password));
				if(con.isValid(10)) {
					isValid = true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		} else if(type.equals(this.SQLSERVER)) {
			try {
				Class.forName(this.SQLSERVER_DRIVER);
				
				//Connection URL format: jdbc:sqlserver://<hostname>[:port];databaseName=<DBname>;user=username;password=password				
				con = DriverManager
						.getConnection(url + ";" + "user=" + username + ";" + "password=" + new String(password));
				if(con.isValid(10)) {
					isValid = true;
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		} else if(type.equals(this.ASTER)) {
			try {
				Class.forName(this.ASTER_DRIVER);
				
				//Connection URL: jdbc:teradata://<HostName>/DATABASE=<DBName>
				con = DriverManager.getConnection(url, username, new String(password));
				if(con != null) {
					isValid = true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return isValid;
	}
	
	public boolean processRDBMSSchema(String type, String url, String username, char[] password)
	{
		boolean success = true;
		if(!url.contains("jdbc") || url.contains("<") || url.contains(">") || url.contains("[") || url.contains("]")) {
			return (success = false);
		}

		Connection con;
		String dbName = "";
		String sql = "";
		Hashtable<String, Hashtable<String, ArrayList<String>>> schemaHash = new Hashtable<String, Hashtable<String, ArrayList<String>>>();
		ResultSet resultSet = null;
		Statement statement  = null;
		try{
			if(type.equals(this.MYSQL)) 
			{
				try {
					Class.forName(this.MYSQL_DRIVER);
					con = DriverManager
							.getConnection(url + "?user=" + username + "&password=" + new String(password));
					//Get DBname from URL
					dbName = url.substring(url.lastIndexOf("/")+1);
					sql = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + dbName + "';";
					logger.info("SQL Query for all Tables/Columns/DataTypes: " + sql);
					statement = con.createStatement();
					resultSet = statement.executeQuery(sql);
				}
				catch (ClassNotFoundException e) {
					e.printStackTrace();
					return (success = false);
				} catch (SQLException e) {
					e.printStackTrace();
					return (success = false);
				}
			}
			else if(type.equals(this.ORACLE)) 
			{
				try {
					Class.forName(this.ORACLE_DRIVER);
					
					if(url.contains("-")) {
						// Get DBname from URL
						dbName = url.substring(url.lastIndexOf("-")+1, url.length());
						// Modify url for connection
						url = url.substring(0, url.indexOf("-"));
					}
					
					con = DriverManager
							.getConnection(url, username, new String(password));
					
					sql = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ALL_TAB_COLUMNS ";
					
					if(!dbName.isEmpty()) {
						sql += "WHERE OWNER=\'" + dbName + "\'";
					}
					
					logger.info("SQL Query for all Tables/Columns/DataTypes: " + sql);
					statement = con.createStatement();
					resultSet = statement.executeQuery(sql);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					return (success = false);
				} catch (SQLException e) {
					e.printStackTrace();
					return (success = false);
				}
			} 
			else if(type.equals(this.SQLSERVER)) 
			{
				try {
					Class.forName(this.SQLSERVER_DRIVER);
					con = DriverManager
							.getConnection(url + ";" + "user=" + username + ";" + "password=" + new String(password));				
					//Get DBname from URL
					dbName = url.substring(url.indexOf("=")+1);
					sql = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG = '" + dbName + "';";
					logger.info("SQL Query for all Tables/Columns/DataTypes: " + sql);
					statement = con.createStatement();
					resultSet = statement.executeQuery(sql);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					return (success = false);
				} catch (SQLException e) {
					e.printStackTrace();
					return (success = false);
				}
			} else if(type.equals(this.ASTER)) {
				try {
					Class.forName(this.ASTER_DRIVER);
					con = DriverManager.getConnection(url, username, new String(password));
					
					DatabaseMetaData md = con.getMetaData();
					resultSet = md.getColumns(null, null, null, null);
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
					return (success = false);
				} catch (SQLException e) {
					e.printStackTrace();
					return (success = false);
				}
			}

			try {
				while(resultSet.next())
				{
					String tableName = "";
					String columnName = "";
					String dataType = "";
					
					if(type.equals(this.ASTER)) {
						tableName = resultSet.getString("TABLE_NAME");
						if(tableName.startsWith("nc_")) {
							continue;
						}
						columnName = resultSet.getString("COLUMN_NAME");
						System.err.println(tableName + " " + columnName);
						dataType = resultSet.getString("DATA_TYPE");
					} else {
						tableName = resultSet.getString(1);
						columnName = resultSet.getString(2);
						dataType = resultSet.getString(3);
					}
					
					logger.debug("SQL Result:     " + tableName + ">>>>>" + columnName + ">>>>>" + dataType);

					if(!schemaHash.containsKey(tableName))
					{
						ArrayList<String> columnList = new ArrayList<String>();
						columnList.add(columnName);
						ArrayList<String> dataTypeList = new ArrayList<String>();
						dataTypeList.add(dataType);
						schemaHash.put(tableName, new Hashtable<String, ArrayList<String>>());
						Hashtable<String, ArrayList<String>> innerHash = schemaHash.get(tableName);
						innerHash.put("COLUMN", columnList);
						innerHash.put("DATATYPE", dataTypeList);
					}
					else
					{
						Hashtable<String, ArrayList<String>> innerHash = schemaHash.get(tableName);
						ArrayList<String> columnList = innerHash.get("COLUMN");
						columnList.add(columnName);
						ArrayList<String> dataTypeList = innerHash.get("DATATYPE");
						dataTypeList.add(dataType);
					}
				}
			} catch (SQLException e1) {
				e1.printStackTrace();
				return (success = false);
			}

			String path = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/rdbms/";
			String excelLoc = path + "RDBMS_Import_Sheet.xlsx";

			XSSFWorkbook wb = null;
			try {
				wb = new XSSFWorkbook(new FileInputStream(excelLoc));
			} catch (IOException e) {
				e.printStackTrace();
				return (success = false);
			}
			
			
			// add schema sheet
			XSSFSheet schemaSheet = wb.createSheet(dbName + "_Schema");
			XSSFRow row = schemaSheet.createRow(0);
			row.createCell(0).setCellValue("TABLE NAME");
			row.createCell(1).setCellValue("COLUMN NAME");
			row.createCell(2).setCellValue("COLUMN DATA TYPE");

			int counter = 1;
			for(String tName : schemaHash.keySet())
			{
				Hashtable<String, ArrayList<String>> innerHash = schemaHash.get(tName);
				ArrayList<String> columnList = innerHash.get("COLUMN");
				ArrayList<String> dataTypeList = innerHash.get("DATATYPE");

				for(int i = 0; i < columnList.size(); i++)
				{
					row = schemaSheet.createRow(counter); 
					row.createCell(0).setCellValue(tName);
					row.createCell(1).setCellValue(columnList.get(i));
					row.createCell(2).setCellValue(dataTypeList.get(i));
					counter++;
				}			
			}
			
			String dataSheetSuffix = "_Data";
			String dataSheetName = dbName;
			if(dataSheetName.length() + dataSheetSuffix.length() > this.EXCEL_WORKSHEET_NAME_CHARACTER_LIMIT) {
				dataSheetName = dbName.substring(0, 25);
			}
			XSSFSheet dataSheet = wb.createSheet(dataSheetName + dataSheetSuffix);
			buildDataSheeet(dataSheet, schemaHash);
			dataSheet.getWorkbook().setSheetHidden(4, true);
			
			// create drop downs for node and relationship tabs
			
			HashSet<String> allColumnNames = new HashSet<String>();
			HashSet<String> allDataTypes = new HashSet<String>();
			String[] tableNames = new String[schemaHash.keySet().size()];
			
			// remove duplicated results
			counter = 0;
			for(String tName : schemaHash.keySet())
			{
				tableNames[counter] = tName; 
				
				Hashtable<String, ArrayList<String>> innerHash = schemaHash.get(tName);
				ArrayList<String> columnList = innerHash.get("COLUMN");
				ArrayList<String> dataTypeList = innerHash.get("DATATYPE");
				
				allColumnNames.addAll(columnList);
				allDataTypes.addAll(dataTypeList);
				counter++;
			}
			allColumnNames.toArray(new String[allColumnNames.size()]);
			allDataTypes.toArray(new String[allDataTypes.size()]);
			
			XSSFSheet nodeSheet = wb.getSheet("Nodes");
			XSSFSheet relationshipSheet = wb.getSheet("Relationships");
			
			// create drop down validation helper
			XSSFDataValidationHelper nodeSheetValidationHelper = new XSSFDataValidationHelper(nodeSheet);
			// create all lists
			DataValidationConstraint nodeSheetTableNameConstraint = nodeSheetValidationHelper.createFormulaListConstraint("TABLES");
			DataValidationConstraint nodeSheetColumnNameConstraint = nodeSheetValidationHelper.createFormulaListConstraint("INDIRECT(UPPER(A2))");
			DataValidationConstraint nodeSheetDataTypeConstraint = nodeSheetValidationHelper.createFormulaListConstraint("INDIRECT(CONCATENATE(UPPER(A2),\"_\",UPPER(C2)))");
			// create all ranges
			CellRangeAddressList nodeSheetTableNameAddressList = new CellRangeAddressList(1,6,0,0);
			CellRangeAddressList nodeSheetColumnNameAddressList1 = new CellRangeAddressList(1,6,1,1);
			CellRangeAddressList nodeSheetColumnNameAddressList2 = new CellRangeAddressList(1,6,2,2);
			CellRangeAddressList nodeSheetDataTypeAddressList = new CellRangeAddressList(1,6,3,3);
			// create the drop downs
			DataValidation nodeSheetTableNameDataValidation = nodeSheetValidationHelper.createValidation(nodeSheetTableNameConstraint, nodeSheetTableNameAddressList);
			DataValidation nodeSheetColumnNameDataValidation1 = nodeSheetValidationHelper.createValidation(nodeSheetColumnNameConstraint, nodeSheetColumnNameAddressList1);
			DataValidation nodeSheetColumnNameDataValidation2 = nodeSheetValidationHelper.createValidation(nodeSheetColumnNameConstraint, nodeSheetColumnNameAddressList2);
			DataValidation nodeSheetDataTypeDataValidation = nodeSheetValidationHelper.createValidation(nodeSheetDataTypeConstraint, nodeSheetDataTypeAddressList);
			// create the drop down side btn
			nodeSheetTableNameDataValidation.setSuppressDropDownArrow(true);
			nodeSheetColumnNameDataValidation1.setSuppressDropDownArrow(true);
			nodeSheetColumnNameDataValidation2.setSuppressDropDownArrow(true);
			nodeSheetDataTypeDataValidation.setSuppressDropDownArrow(true);
			// add the validation to the node sheet
			nodeSheet.addValidationData(nodeSheetTableNameDataValidation);
			nodeSheet.addValidationData(nodeSheetColumnNameDataValidation1);
			nodeSheet.addValidationData(nodeSheetColumnNameDataValidation2);
			nodeSheet.addValidationData(nodeSheetDataTypeDataValidation);
			
			
			// create drop down validation helper
			XSSFDataValidationHelper relationshipSheetValidationHelper = new XSSFDataValidationHelper(relationshipSheet);
			// create all lists
			DataValidationConstraint relationshipSheetTableNameConstraint = relationshipSheetValidationHelper.createFormulaListConstraint("TABLES");
			DataValidationConstraint relationshipSheetColumnNameConstraint1 = relationshipSheetValidationHelper.createFormulaListConstraint("INDIRECT(UPPER(A2))");
			DataValidationConstraint relationshipSheetColumnNameConstraint2 = relationshipSheetValidationHelper.createFormulaListConstraint("INDIRECT(UPPER(D2))");
			DataValidationConstraint relationshipSheetColumnNameConstraint3 = relationshipSheetValidationHelper.createFormulaListConstraint("INDIRECT(UPPER(G2))");

			// create all ranges
			CellRangeAddressList relationshipSheetTableNameAddressList1 = new  CellRangeAddressList(1,6,0,0);
			CellRangeAddressList relationshipSheetTableNameAddressList2 = new  CellRangeAddressList(1,6,3,3);
			CellRangeAddressList relationshipSheetTableNameAddressList3 = new  CellRangeAddressList(1,6,6,6);
			
			CellRangeAddressList relationshipSheetColumnNameAddressList1 = new CellRangeAddressList(1,6,1,1);
			CellRangeAddressList relationshipSheetColumnNameAddressList2 = new CellRangeAddressList(1,6,2,2);
			CellRangeAddressList relationshipSheetColumnNameAddressList3 = new CellRangeAddressList(1,6,4,4);
			CellRangeAddressList relationshipSheetColumnNameAddressList4 = new CellRangeAddressList(1,6,5,5);
			CellRangeAddressList relationshipSheetColumnNameAddressList5 = new CellRangeAddressList(1,6,7,7);
			CellRangeAddressList relationshipSheetColumnNameAddressList6 = new CellRangeAddressList(1,6,8,8);

			// create the drop downs	    
			DataValidation relationshipSheetTableNameDataValidation1 = relationshipSheetValidationHelper.createValidation(relationshipSheetTableNameConstraint, relationshipSheetTableNameAddressList1);
			DataValidation relationshipSheetTableNameDataValidation2 = relationshipSheetValidationHelper.createValidation(relationshipSheetTableNameConstraint, relationshipSheetTableNameAddressList2);
			DataValidation relationshipSheetTableNameDataValidation3 = relationshipSheetValidationHelper.createValidation(relationshipSheetTableNameConstraint, relationshipSheetTableNameAddressList3);
			DataValidation relationshipSheetColumnNameDataValidation1 = relationshipSheetValidationHelper.createValidation(relationshipSheetColumnNameConstraint1, relationshipSheetColumnNameAddressList1);
			DataValidation relationshipSheetColumnNameDataValidation2 = relationshipSheetValidationHelper.createValidation(relationshipSheetColumnNameConstraint1, relationshipSheetColumnNameAddressList2);
			DataValidation relationshipSheetColumnNameDataValidation3 = relationshipSheetValidationHelper.createValidation(relationshipSheetColumnNameConstraint2, relationshipSheetColumnNameAddressList3);
			DataValidation relationshipSheetColumnNameDataValidation4 = relationshipSheetValidationHelper.createValidation(relationshipSheetColumnNameConstraint2, relationshipSheetColumnNameAddressList4);
			DataValidation relationshipSheetColumnNameDataValidation5 = relationshipSheetValidationHelper.createValidation(relationshipSheetColumnNameConstraint3, relationshipSheetColumnNameAddressList5);
			DataValidation relationshipSheetColumnNameDataValidation6 = relationshipSheetValidationHelper.createValidation(relationshipSheetColumnNameConstraint3, relationshipSheetColumnNameAddressList6);
			// create the drop down side btn
			relationshipSheetTableNameDataValidation1.setSuppressDropDownArrow(true);
			relationshipSheetTableNameDataValidation2.setSuppressDropDownArrow(true);
			relationshipSheetTableNameDataValidation3.setSuppressDropDownArrow(true);
			relationshipSheetColumnNameDataValidation1.setSuppressDropDownArrow(true);
			relationshipSheetColumnNameDataValidation2.setSuppressDropDownArrow(true);
			relationshipSheetColumnNameDataValidation3.setSuppressDropDownArrow(true);
			relationshipSheetColumnNameDataValidation4.setSuppressDropDownArrow(true);
			relationshipSheetColumnNameDataValidation5.setSuppressDropDownArrow(true);
			relationshipSheetColumnNameDataValidation6.setSuppressDropDownArrow(true);
			// add the validations to the relationship sheet
			relationshipSheet.addValidationData(relationshipSheetTableNameDataValidation1);
			relationshipSheet.addValidationData(relationshipSheetTableNameDataValidation2);
			relationshipSheet.addValidationData(relationshipSheetTableNameDataValidation3);
			relationshipSheet.addValidationData(relationshipSheetColumnNameDataValidation1);
			relationshipSheet.addValidationData(relationshipSheetColumnNameDataValidation2);
			relationshipSheet.addValidationData(relationshipSheetColumnNameDataValidation3);
			relationshipSheet.addValidationData(relationshipSheetColumnNameDataValidation4);
			relationshipSheet.addValidationData(relationshipSheetColumnNameDataValidation5);
			relationshipSheet.addValidationData(relationshipSheetColumnNameDataValidation6);

			try {
				wb.write(new FileOutputStream(path + dbName + "_" + "RDBMS_Import_Sheet.xlsx"));
			} catch (FileNotFoundException e) {
				e.printStackTrace();	
				return (success = false);
			} catch (IOException e) {
				e.printStackTrace();
				return (success = false);
			}
		 
	} catch (Exception e ){
		e.printStackTrace();
	} finally {
		ConnectionUtils.closeAllConnections(null, resultSet, statement);
	}

		return success;
	}
	
	private void buildDataSheeet(XSSFSheet dataSheet, Hashtable<String, Hashtable<String, ArrayList<String>>> schemaHash) 
	{
		int rowNum = 1;
		int colColumnNum = 0;
		int totalRowsOfColumns = 0;
		
		ArrayList<String> tableNameList = new ArrayList<String>();
		ArrayList<String> table_columnNameList = new ArrayList<String>();
		XSSFRow tableNameRow = dataSheet.createRow(0);
		
		int cellTableNum = 0;
		for( String tableName : schemaHash.keySet() )
		{
			tableNameList.add(tableName);
			
			tableNameRow.createCell(cellTableNum).setCellValue(tableName);
			cellTableNum++;
			
			Hashtable<String, ArrayList<String>> innerHash = schemaHash.get(tableName);
			ArrayList<String> columnList = innerHash.get("COLUMN");
			
			for( String colName : columnList)
			{
				table_columnNameList.add(tableName + "_" + colName);
				
				XSSFRow currColRow = dataSheet.getRow(rowNum);
				if(currColRow == null)
				{
					currColRow = dataSheet.createRow(rowNum);
					currColRow.createCell(colColumnNum).setCellValue(colName);
				}
				else
				{
					currColRow.createCell(colColumnNum).setCellValue(colName);
				}
				colColumnNum++;
			}
			colColumnNum = 0;
			rowNum++;
		}
		totalRowsOfColumns = rowNum - 1;
		
		for( String tableName : schemaHash.keySet() )
		{
			Hashtable<String, ArrayList<String>> innerHash = schemaHash.get(tableName);
			ArrayList<String> dataTypeList = innerHash.get("DATATYPE");
			
			for( String dataTypeName : dataTypeList)
			{
				XSSFRow currColRow = dataSheet.createRow(rowNum);
				currColRow.createCell(0).setCellValue(dataTypeName);
				rowNum++;
			}
		}
			
		char tableColStart = 'A';
		int currRowNum = 1;
		int internalTableCounter = 0;
		int internalTableColumnCounter = 0;
		for(int processRow = 0; processRow <= dataSheet.getLastRowNum(); processRow++)
		{
			XSSFRow currRow = dataSheet.getRow(processRow);
			
			if(processRow == 0)
			{
				char endRowCol = (char) (tableColStart + currRow.getLastCellNum() - 1);
				Name nameTable = dataSheet.getWorkbook().createName();
				nameTable.setNameName("TABLES");
				nameTable.setRefersToFormula("'" + dataSheet.getSheetName() +"'!" + "$" + tableColStart + "$" + currRowNum + ":$" + endRowCol + "$" + currRowNum);
			}
			else if (1 <= processRow && processRow <= totalRowsOfColumns)
			{
				char endRowCol = (char) (tableColStart + currRow.getLastCellNum() - 1);

				Name nameCol = dataSheet.getWorkbook().createName();
				nameCol.setNameName(tableNameList.get(internalTableCounter));
				nameCol.setRefersToFormula("'" + dataSheet.getSheetName() +"'!" + "$" + tableColStart + "$" + currRowNum + ":$" + endRowCol + "$" + currRowNum);
				internalTableCounter++;
			}
			else
			{
				char endRowCol = 'A';
				
				Name nameCol = dataSheet.getWorkbook().createName();
				nameCol.setNameName(table_columnNameList.get(internalTableColumnCounter));
				nameCol.setRefersToFormula("'" + dataSheet.getSheetName() +"'!" + "$" + tableColStart + "$" + currRowNum + ":$" + endRowCol + "$" + currRowNum);
				internalTableColumnCounter++;
			}
			currRowNum++;
		}
	}

	
//	private void buildDataSheeet(XSSFSheet dataSheet)
//	{
//		int numRows = dataSheet.getLastRowNum();
//	
//		char tableCol = 'A';
//		
//		Name nameTable = dataSheet.getWorkbook().createName();
//		nameTable.setNameName("TABLES");
//		nameTable.setRefersToFormula("'" + dataSheet.getSheetName() +"'!" + "$" + tableCol + "$" + 2 + ":$" + tableCol + "$" + numRows+1);
//		
//		int row = 1;
//		String lastTableName = "";
//		int colsForTable = 0;
//		int colStartRow = 2;
//		char columnCol = 'B';
//
//		
//		for(int i = 0; i < numRows; i++)
//		{
//			XSSFRow currRow = dataSheet.getRow(row);
//			String currTableName = currRow.getCell(0).getStringCellValue();
//			if(lastTableName.equals(""))
//			{
//				lastTableName = currTableName;
//				colsForTable++;
//			}
//			else if(lastTableName.equals(currTableName))
//			{
//				colsForTable++;
//			}
//			else
//			{				
//				int endRow = colStartRow+colsForTable-1;
//				Name nameCol = dataSheet.getWorkbook().createName();
//				nameCol.setNameName(lastTableName);
//				nameCol.setRefersToFormula("'" + dataSheet.getSheetName() +"'!" + "$" + columnCol + "$" + colStartRow + ":$" + columnCol + "$" + endRow );
//				
//				int dataCounterRow = colStartRow;
//				while(dataCounterRow <= endRow)
//				{
//					char dataTypeCol = 'C';
//					String dataTypeName = dataSheet.getRow(dataCounterRow).getCell(1).getStringCellValue();
//					Name nameData = dataSheet.getWorkbook().createName();
//					nameData.setNameName(lastTableName+"_"+dataTypeName);
//					nameData.setRefersToFormula("'" + dataSheet.getSheetName() +"'!" + "$" + dataTypeCol + "$" + dataCounterRow + ":$" + dataTypeCol + "$" + dataCounterRow);
//					
//					dataCounterRow++;
//				}
//				
//				colStartRow = row + 1;
//				colsForTable = 1;
//				lastTableName = currTableName;
//			}
//			row++;
//		}
//	}
}