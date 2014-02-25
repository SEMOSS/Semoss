package prerna.rdf.main;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.util.Constants;
import prerna.util.Utility;

public class D2RQMappingGenerationTest {
	
	private StringBuilder tableMapping = new StringBuilder();
	private StringBuilder propertyTypeMapping = new StringBuilder();
	private StringBuilder relationshipTypeMapping = new StringBuilder();
	private StringBuilder relationshipMapping = new StringBuilder();
	private String dbConnection = new String();
	
	private String customBaseURI = "http://health.mil/ontologies/" + Constants.DEFAULT_NODE_CLASS + "/";
	private String baseRelURI = "http://health.mil/ontologies/" + Constants.DEFAULT_RELATION_CLASS + "/";
	private final String semossURI = "http://semoss.org/ontologies" + "/" + Constants.DEFAULT_NODE_CLASS + "/";
	private final String semossRelURI = "http://semoss.org/ontologies" + "/" + Constants.DEFAULT_RELATION_CLASS + "/";
	private final String propURI = "http://semoss.org/ontologies/" + Constants.DEFAULT_RELATION_CLASS + "/Contains/";
	private String filePath = "";
	private String dbName = "";
	private String url = "";
	private String username = "";
	private String password = "";
	
	private final static String spacer = " \n\t";

	private Set<String> propertyList = new HashSet<String>();
	private Set<String> relationshipList = new HashSet<String>();
	private Set<String> baseConcepts = new HashSet<String>();
	private Set<String> baseRels = new HashSet<String>();
	private Hashtable<String, ArrayList<String>> baseRelationships = new Hashtable<String, ArrayList<String>>();
	
	public D2RQMappingGenerationTest(String customBaseURI, String fileNames, String repoName, String url, String username, String password) {
		this.customBaseURI = customBaseURI;
		this.filePath = fileNames;
		this.dbName = repoName;
		this.url = url;
		this.username = username;
		this.password = password;
	}
	
	public void createMappingFile()
	{
		processExcel(this.filePath);
		
		//////////////////////////////////Change path for where the template file is
		String templatePath = System.getProperty("user.dir") + "/rdbms/MappingTemplate.ttl";
		System.err.println("Template Path: " + templatePath);
		String requiredMapping = readRequiredMappings(templatePath);
		
		// Write the file
		//////////////////////////////////Change path for where you want the file
		File mappingFile = new File(System.getProperty("user.dir") + "/db/" + this.dbName);
		if(!mappingFile.exists()){
			try {
				mappingFile.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try{
			FileWriter writer = new FileWriter(mappingFile.getAbsolutePath());
			writer.write("@prefix map: <#> . \n");
			writer.write("@prefix d2rq: <http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#> . \n");
			writer.write("@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . \n");
			writer.write("@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . \n");
			writer.write("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> . \n");
			writer.write("\n");
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
	}
	
	private void processExcel(String wb){
		XSSFWorkbook workbook = null;
		try {
			workbook = new XSSFWorkbook(new FileInputStream(wb));
		} catch (Exception e) {
			e.printStackTrace();
			Utility.showError("Couldn't Find Workbook");
		}
		
		// process properties
		XSSFSheet propSheet = workbook.getSheet("Properties");
		
		// check rows in correct order
		XSSFRow headerPropRow = propSheet.getRow(0);
		if(!headerPropRow.getCell(0).toString().equals("Table") && !headerPropRow.getCell(0).toString().equals("Subject") && !headerPropRow.getCell(0).toString().equals("Property") && !headerPropRow.getCell(0).toString().equals("DataType")){
			Utility.showError("Headers are incorrect in property sheet! \nPlease correct your workbook format");
		}
		
		String tableInput = "";
		String tableInstanceColumn = "";
		String propertyName = "";
		String dataType = "";
		String nodeType = "";
		
		int propRows = propSheet.getLastRowNum();
		for(int i = 1; i < propRows; i++){
			XSSFRow dataRow = propSheet.getRow(i);
			tableInput = dataRow.getCell(0).toString();
			tableInstanceColumn = dataRow.getCell(1).toString();
			propertyName = dataRow.getCell(2).toString();
			
			dataType = dataRow.getCell(3).toString();
			nodeType = dataRow.getCell(4).toString();
			baseConcepts.add(nodeType);
						
			ProcessTable(tableInput, tableInstanceColumn);
			
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
		for(int i = 1; i < relRows; i++){
			XSSFRow dataRow = relSheet.getRow(i);
			relTable = dataRow.getCell(0).toString();;
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
			
			processRelationships(relTable, relSubjectColumn, relObjectColumn, subjectTable, objectTable, relation, subjectInstance, objectInstance, subjectID, objectID);
		}
		
		processRelationshipType(relationshipList);
	}

	//TODO: when is the user passing in this information?
	private String createDatabase(String url, String username, String password)
	{
		return dbConnection = "map:database a d2rq:Database;" + spacer + 
				"d2rq:jdbcDSN \"jdbc:mysql://" + url + "\";" + spacer + 
				"d2rq:jdbcDriver \"com.mysql.jdbc.Driver\";" + spacer + 
				"d2rq:username \"" + username + "\";" + spacer + 
				"d2rq:password \"" + password + "\";" + spacer + 
				"jdbc:keepAlive \"3600\";" + spacer + 
				".\n";
	}
	
	private void ProcessTable(String tableName, String tableInstance){
		tableMapping.append("#####Table " + tableName + "\n" +
				"#Create the instanceNode typeOf baseNode triple \n" +
				"map:Instance" + tableName + "_TypeOf_Base" + tableName + " a d2rq:ClassMap;" + spacer + 
				"d2rq:dataStorage map:database;" + spacer + 
				"d2rq:uriPattern \"" + customBaseURI + tableName + "/@@" + tableName +"." + tableInstance + "@@\";" + spacer + 
				"d2rq:class " + semossURI + tableName + ";" + spacer + 
				"d2rq:additionalProperty map:TypeOf_Concept;" + spacer + 
				".\n" +
				"#Create the baseNode subclassOf Concept triple \n" +
				"map:Base" + tableName + "_SubClassOf_Concept a d2rq:ClassMap;" + spacer + 
				"d2rq:dataStorage map:database;" + spacer + 
				"d2rq:constantValue <" + semossURI + tableName + ">;" + spacer + 
				"d2rq:additionalProperty map:SubClassOf_Concept;" + spacer + 
				".\n"); 
	}
	
	private void processTableProperty(String tableName, String propertyName, String dataType){
		
		//TODO: write method if property is a rdfs:label once we determine how user will specify which column is a rdfs:label
		
		//add property to total list of unique properties
		propertyList.add(propertyName);
		
		tableMapping.append("#####Property " + propertyName + " for Table " + tableName + "\n" +
				"#Create the instanceNode contains/prop propValue triple \n" +
				"map:Instance" + tableName +"_BaseProp_" + propertyName + " a d2rq:PropertyBridge; \n" +
				"d2rq:belongsToClassMap map:Instance" + tableName + "_TypeOf_Base" + tableName + ";" + spacer + 
				"d2rq:property " + propURI + propertyName + ";" + spacer + 
				"d2rq:column " + "\"" + tableName + "." + propertyName + "\";" + spacer + 
				"d2rq:dataType xsd:" + dataType + ";" + spacer + 
				".\n");			
	}
	
	private void processProperties(Set<String> propertyList){
		Iterator<String> propIterator = propertyList.iterator();
		while(propIterator.hasNext()){
			String propertyName = propIterator.next();
			propertyTypeMapping.append("#####Property " + propertyName + "\n" +
				"#Create the Necessary Definitions for the property " + propertyName +"\n" +
				"map:Base" + propertyName + " a d2rq:ClassMap;" + spacer + 
				"d2rq:dataStorage map:database;" + spacer + 
				"d2rq:constantValue <" + propURI + propertyName + ">;" + spacer + 
				"d2rq:additionalProperty map:TypeOf_Property;" + spacer + 
				"d2rq:additionalProperty map:TypeOf_Contains;" + spacer + 
				"d2rq:additionalProperty map:Base" + propertyName + "_SubPropertyOf_Base" + propertyName + ";" + spacer + 
				".\n" +
				"map:Base" + propertyName + "_SubPropertyOf_Base" + propertyName + " a d2rq:AdditionalProperty;" + spacer + 
				"d2rq:propertyName rdfs:subPropertyOf;" + spacer + 
				"d2rq:propertyValue <" + propURI + propertyName + ">;" + spacer + 
				".\n");
		}
	}
	
	private void processRelationships(String relTable, String relSubjectColumn, String relObjectColumn, String subjectTable, String objectTable, String relation, String subjectInstance, String objectInstance, String subjectID, String objectID){
		
		//add relationship to total list of unique relationships
		relationshipList.add(relation);
		
		relationshipMapping.append("#####Defining Relationship: " + subjectTable + " " + relation + " " + objectTable + "\n" +
			"#Create the instance " + subjectTable + " " + relation + " " + objectTable + "\n" + 
			"map:Instance" + subjectTable + "_InstanceRel_Instance" + objectTable + " a d2rq:PropertyBridge;" + spacer + 
			"d2rq:belongsToClassMap map:Instance" + subjectTable + "_TypeOf_Base" + subjectTable + ";" + spacer + 
			"d2rq:refersToClassMap map:Instance" + objectTable + "TypeOf_Base" + objectTable + ";" + spacer + 
			"d2rq:dynamicProperty \"http://" + baseRelURI + relation + "/@@" + subjectTable + "." + subjectInstance + "@@:@@" + objectTable + "." + objectInstance + "@@\";" + spacer + 
			"d2rq:join \"" + relTable + "." + relSubjectColumn + " = " + subjectTable +"." + subjectID + "\";" + spacer + 
			"d2rq:join \"" + relTable + "." + relObjectColumn + " = " + objectTable +"." + objectID + "\";" + spacer + 
			".\n" + 
			"#Create the higher level triples for the relationship \n" +
			"map:InstanceRel_" + subjectTable + "_" + objectTable + " a d2rq:ClassMap;" + spacer + 
			"d2rq:dataStorage map:database;" + spacer + 
			"d2rq:uriPattern \"http://" + baseRelURI + relation + "/@@" + subjectTable + "." + subjectInstance + "@@:@@" + objectTable + "." + objectInstance + "@@\";" + spacer + 
			"d2rq:join \"" + relTable + "." + relSubjectColumn + " = " + subjectTable +"." + subjectID + "\";" + spacer + 
			"d2rq:join \"" + relTable + "." + relObjectColumn + " = " + objectTable +"." + objectID + "\";" + spacer + 
			"d2rq:additionalProperty map:TypeOf_Property;" + spacer + 
			"d2rq:additionalProperty map:SubPropertyOf_Relation;" + spacer + 
			"d2rq:additionalProperty map:SubPropertyOf_" + relation + "_" + subjectTable + "_" + objectTable + ";" + spacer + 
			".\n" +
			"map:SubPropertyOf_" + relation + "_" + subjectTable + "_" + objectTable + "a d2rq:AdditionalProperty;" + spacer + 
			"d2rq:propertyName rdfs:subPropertyOf;" + spacer + 
			"d2rq:propertyValue <" + semossRelURI + relation + ">;" + spacer + 
			".\n" + 
			"map:Label_" + relation + "_" + subjectTable + "_" + objectTable + "a d2rq:PropertyBridge;" + spacer + 
			"d2rq:belongsToClassMap map:InstanceRel_" + subjectTable + "_" + objectTable + ";" + spacer + 
			"d2rq:property rdfs:label;" + spacer + 
			"d2rq:pattern \"@@" + subjectTable + "." + subjectInstance + "@@.@@" + objectTable + "." + objectInstance +"\">;" + spacer + 
			".\n" +
			"map:" + subjectTable + "_" + objectTable + "_SubPropertyOf_Self a d2rq:PropertyBridge;" + spacer + 
			"d2rq:belongsToClassMap map:InstanceRel_" + subjectTable + "_" + objectTable + ";" + spacer + 
			"d2rq:property rdfs:subPropertyOf;" + spacer + 
			"d2rq:uriPattern \"http://" + baseRelURI + relation + "/@@" + subjectTable + "." + subjectInstance + "@@:@@" + objectTable + "." + objectInstance + "@@\";" + spacer + 
			"d2rq:join \"" + relTable + "." + relSubjectColumn + " = " + subjectTable +"." + subjectID + "\";" + spacer + 
			"d2rq:join \"" + relTable + "." + relObjectColumn + " = " + objectTable +"." + objectID + "\";" + spacer + 
			".\n" +
			"map:Instance" + subjectTable + "_Rel_Instance_" + objectTable +" a d2rq:PropertyBridge;" + spacer + 
			"d2rq:belongsToClassMap map:Instance" + subjectTable + "_TypeOf_Base" + subjectTable + ";" + spacer + 
			"d2rq:refersToClassMap map:Instance" + objectTable + "TypeOf_Base" + objectTable + ";" + spacer + 
			"d2rq:property <http://semoss.org/ontologies/Relation>;" + spacer + 
			"d2rq:join \"" + relTable + "." + relSubjectColumn + " = " + subjectTable +"." + subjectID + "\";" + spacer + 
			"d2rq:join \"" + relTable + "." + relObjectColumn + " = " + objectTable +"." + objectID + "\";" + spacer + 
			".\n" +
			"map:Instance" + subjectTable + "_Rel_" + objectTable +" a d2rq:PropertyBridge;" + spacer + 
			"d2rq:belongsToClassMap map:Instance" + subjectTable + "_TypeOf_Base" + subjectTable + ";" + spacer + 
			"d2rq:refersToClassMap map:Instance" + objectTable + "TypeOf_Base" + objectTable + ";" + spacer + 
			"d2rq:property <" + semossRelURI + relation + ">;" + spacer + 
			"d2rq:join \"" + relTable + "." + relSubjectColumn + " = " + subjectTable +"." + subjectID + "\";" + spacer + 
			"d2rq:join \"" + relTable + "." + relObjectColumn + " = " + objectTable +"." + objectID + "\";" + spacer + 
			".\n");
	}
	
	
	private void processRelationshipType(Set<String> relationshipList){
		Iterator<String> relIterator = relationshipList.iterator();
		while(relIterator.hasNext()){
			String relationshipName = relIterator.next();
			relationshipTypeMapping.append("#####Relationship " + relationshipName + "\n" +
				"#Create the rel/relName subPropertyOf rel triple \n" +
				"d2rq:dataStorage map:database;" + spacer + 
				"d2rq:constantValue <" + semossRelURI + relationshipName + ">;" + spacer + 
				"d2rq:additionalProperty map:SubPropertyOf_Relation;" + spacer + 
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
		owlFile.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?> \n" +
				"<rdf:RDF" + spacer + 
					"xmlns=\"http://semoss.org/ontologies/Relation\"" + spacer + 
					"xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"" + spacer + 
					"xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"> \n" +
				"<rdfs:Class rdf:about=\"http://semoss.org/ontologies/Concept\"/> \n" +
				"<rdf:Property rdf:about=\"http://semoss.org/ontologies/Relation\"/>) \n");
		
		for(String conceptName : baseConcepts)
		{
			owlFile.append("<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/" + conceptName + "\">" + spacer + 
				"rdfs:subClassOf rdf:resource=\"http://semoss.org/ontologies/Concept\"/> \n");
		}
		
		for(String relName : baseRels)
		{
			owlFile.append("<rdf:Description rdf:about=\"http://semoss.org/ontologies/Relation/" + relName + "\">" + spacer + 
				"<rdfs:subPropertyOf rdf:resource=\"http://semoss.org/ontologies/Relation\"/> \n");
		}
		
		for(String key: baseRelationships.keySet())
		{
			ArrayList<String> relation = baseRelationships.get(key);
			String sub = relation.get(0);
			String rel = relation.get(1);
			String obj = relation.get(2);
			
			owlFile.append("<rdf:Description rdf:about=\"http://semoss.org/ontologies/Concept/" + sub + "\"> +"
					+ "<" + rel + " rdf:resource=\"http://semoss.org/ontologies/Concept/" + obj + "\"/> \n");
		}
		
		try{
			FileWriter owlWriter = new FileWriter(path);
			owlWriter.write(owlFile.toString());
			owlWriter.close();
		} catch (IOException e) {
			Utility.showError("Could not create owl file!");
			e.printStackTrace();
		}
	}
}
