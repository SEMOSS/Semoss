package prerna.ui.comparison.specific.tap;

import java.util.ArrayList;

import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.error.EngineException;
import prerna.rdf.engine.api.IEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class GenericDBComparisonWriter
{
	// Instance level queries
	final String getConceptsAndInstanceCountQuery = "SELECT DISTINCT ?concept (COUNT(DISTINCT ?instance) AS ?count) WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?concept} } GROUP BY ?concept";
	final String getInstanceAndPropCountQuery = "SELECT DISTINCT ?nodeType ?source (COUNT(DISTINCT ?entity) AS ?entityCount) WHERE { FILTER(?nodeType != <http://semoss.org/ontologies/Concept>){?nodeType <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?source <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?nodeType} {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?source ?entity ?prop } } GROUP BY ?nodeType ?source";
	final String getInstanceInRelationCountQuery = "SELECT DISTINCT ?concept ?instance (COUNT(?inRel) AS ?totalIn) WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?concept} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?inRel <http://www.w3.org/2000/01/rdf-schema#label> ?label} {?node2 ?inRel ?instance} } GROUP BY ?concept ?instance";
	final String getInstanceOutRelationCountQuery = "SELECT DISTINCT ?concept ?instance (COUNT(?outRel) AS ?totalOut) WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?concept} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?outRel <http://www.w3.org/2000/01/rdf-schema#label> ?label} {?instance ?outRel ?node} } GROUP BY ?concept ?instance";
	final String getInstanceListQuery = "SELECT DISTINCT ?concept ?instance WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?concept} }";
	final String getInstancePropertyValueQuery = "SELECT DISTINCT ?concept ?instance ?contains ?property WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?concept} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?instance ?contains ?property }} ORDERBY ?concept";
	final String getRelationPropertyValueQuery = "SELECT DISTINCT ?subConcept ?objConcept ?subject ?relation ?object ?contains ?property WHERE { FILTER(?subConcept != <http://semoss.org/ontologies/Concept>) FILTER(?objConcept != <http://semoss.org/ontologies/Concept>) {?subConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?objConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subConcept} {?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?objConcept} {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?relation <http://www.w3.org/2000/01/rdf-schema#label> ?label} {?subject ?relation ?object} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?relation ?contains ?property }}";
	
	// Metamodel levelqueries
	final String getMetaConceptCountQuery = "SELECT DISTINCT (COUNT(DISTINCT ?concept) AS ?conceptCount) WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} }";
	final String getMetaConceptPropertyCountQuery = "SELECT DISTINCT ?concept (COUNT(DISTINCT ?contains) AS ?propertyCount) WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?instance <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?concept} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?instance ?contains ?property } } GROUP BY ?concept";
	final String getMetaConceptInRelationCountQuery = "SELECT DISTINCT ?concept (COUNT(?inRel) AS ?totalIn) WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?inRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?node2 ?inRel ?concept} } GROUP BY ?concept";
	final String getMetaConceptOutRelationCountQuery = "SELECT DISTINCT ?concept (COUNT(?outRel) AS ?totalOut) WHERE { FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?outRel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?concept ?outRel ?node} } GROUP BY ?concept";
	final String getMetaTotalInRelationCountQuery = "SELECT DISTINCT (COUNT(?relation) AS ?relationCount) WHERE {FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?node ?relation ?concept} }";
	final String getMetaTotalOutRelationCountQuery = "SELECT DISTINCT (COUNT(?relation) AS ?relationCount) WHERE {FILTER(?concept != <http://semoss.org/ontologies/Concept>) {?concept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?concept ?relation ?node} }";
	final String getMetaRelationPropertyCountQuery = "SELECT DISTINCT ?subConcept ?relation ?objConcept (COUNT(DISTINCT ?contains) AS ?propertyCount) WHERE { FILTER(?subConcept != <http://semoss.org/ontologies/Concept>) FILTER(?objConcept != <http://semoss.org/ontologies/Concept>) {?subConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?objConcept <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept>} {?subject <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subConcept} {?object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?objConcept} {?relation <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation>} {?relation <http://www.w3.org/2000/01/rdf-schema#label> ?label} {?subject ?relation ?object} {?contains <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Relation/Contains>} {?relation ?contains ?property }} GROUP BY ?subConcept ?relation ?objConcept";
	
	private GenericDBComparer comparer;
	private XSSFWorkbook wb;
	
	private String newDBName;
	private String oldDBName;
	
	private String sheetName = "";
	
	public GenericDBComparisonWriter(IEngine newDB, IEngine oldDB, IEngine newMetaDB, IEngine oldMetaDB) throws EngineException
	{
		comparer = new GenericDBComparer(newDB, oldDB, newMetaDB, oldMetaDB);
		wb = new XSSFWorkbook();
		
		this.newDBName = newDB.getEngineName();
		this.oldDBName = oldDB.getEngineName();
	}
	
	public void runAllInstanceTests()
	{
		sheetName = "InstanceCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareConceptCount(getConceptsAndInstanceCountQuery, false), "Concept", newDBName + " Count", oldDBName
				+ " Count", "Comments");
		
		sheetName = "InstancePropertyCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareInstanceCount(getInstanceAndPropCountQuery), "Concept", "Instance", newDBName + " Count", oldDBName
				+ " Count", "Comments");
		
		sheetName = "InRelationCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareInstanceCount(getInstanceInRelationCountQuery), "Concept", "Instance", newDBName + " Count",
				oldDBName + " Count", "Comments");
		
		sheetName = "OutRelationCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareInstanceCount(getInstanceOutRelationCountQuery), "Concept", "Instance", newDBName + " Count",
				oldDBName + " Count", "Comments");
		
		sheetName = "CaseSensitiveInstanceDuplicates";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.findCaseInstanceDuplicate(getInstanceListQuery), "Concept", "Instance");
		
		sheetName = "InstancePropertyDuplicates";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.findInstancePropertyDuplicate(getInstancePropertyValueQuery), "Concept", "Instance", "Property", "Value");
		
		sheetName = "RelationPropertyDuplicates";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.findRelationPropertyDuplicate(getRelationPropertyValueQuery), "Subject's Concept", "Object's Concept",
				"Subject", "Relation", "Object", "Property", "Value");
		
		System.out.println("All Instance Tests Finished");
	}
	
	public void runAllMetaTests()
	{
		sheetName = "MetaConceptCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareMetaSingleCount(getMetaConceptCountQuery), newDBName + " Count", oldDBName + " Count");
		
		sheetName = "MetaConceptPropertyCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareConceptCount(getMetaConceptPropertyCountQuery, false), "Concept", newDBName + " Count", oldDBName
				+ " Count", "Comments");
		
		sheetName = "MetaConceptInRelationCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareConceptCount(getMetaConceptInRelationCountQuery, true), "Concept", newDBName + " Count", oldDBName
				+ " Count", "Comments");
		
		sheetName = "MetaConceptOutRelationCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareConceptCount(getMetaConceptOutRelationCountQuery, true), "Concept", newDBName + " Count", oldDBName
				+ " Count", "Comments");
		
		sheetName = "MetaTotalInRelationCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareMetaSingleCount(getMetaTotalInRelationCountQuery), newDBName + "Count", oldDBName + "Count");
		
		sheetName = "MetaTotalOutRelationCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareMetaSingleCount(getMetaTotalOutRelationCountQuery), newDBName + "Count", oldDBName + "Count");
		
		sheetName = "MetaRelationPropertyCount";
		wb.createSheet(sheetName);
		writeToExcel(sheetName, comparer.compareMetaRelationPropertyCount(getMetaRelationPropertyCountQuery), "Subject Concept", "Relation",
				"Object Concept", newDBName + " Count", oldDBName + " Count", "Comments");
		
		System.out.println("All Metamodel Tests Finished");
	}
	
	public void writeToExcel(String sheetInUse, ArrayList<Object[]> result, String... colNames)
	{
		XSSFSheet sheet = wb.getSheet(sheetInUse);
		XSSFRow selectedRow = sheet.createRow(0);
		XSSFCell selectedCell;
		for (int i = 0; i < colNames.length; i++)
		{
			selectedCell = selectedRow.createCell(i);
			selectedCell.setCellValue(colNames[i]);
		}
		
		int rowNum = 1;
		
		for (Object[] row : result)
		{
			selectedRow = sheet.createRow(rowNum);
			for (int i = 0; i < row.length; i++)
			{
				selectedCell = selectedRow.createCell(i);
				selectedCell.setCellValue(row[i].toString());
			}
			rowNum++;
		}
	}
	
	public void writeWB()
	{
		String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String folder = System.getProperty("file.separator") + "export" + System.getProperty("file.separator") + "Comparisons"
				+ System.getProperty("file.separator");
		String resultName = newDBName + "~" + oldDBName + "~DBComparisonResults.xlsx";
		Utility.writeWorkbook(wb, workingDir + folder + resultName);
	}
}
