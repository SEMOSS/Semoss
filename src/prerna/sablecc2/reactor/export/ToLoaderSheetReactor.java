package prerna.sablecc2.reactor.export;

import java.io.File;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.PixelPlanner;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ToLoaderSheetReactor extends AbstractReactor {

	private static final String CLASS_NAME = ToLoaderSheetReactor.class.getName();
	
	public ToLoaderSheetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		organizeKeys();
		String engineName = this.keyValue.get(this.keysToGet[0]);
		if(engineName == null) {
			throw new IllegalArgumentException("Must define which database to get a loader sheet from");
		}

		String fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\" + engineName + "_Loader_Sheet_Export.xlsx";
		File f = new File(fileLoc);
		if(f.exists()) {
			f.delete();
		}
		Workbook workbook = new XSSFWorkbook();
		
		IEngine engine = Utility.getEngine(engineName);
		// get a list of all the tables and properties
		Vector<String> concepts = engine.getConcepts(true);

		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String conceptualName = Utility.getInstanceName(concept);
			
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
			qs.setEngine(engine);
			qs.addSelector(new QueryColumnSelector(conceptualName));
			
			List<String> properties = engine.getProperties4Concept(concept, true);
			for(String property : properties) {
				// THIS IS BECAUSE THERE ARE OTHER ERRORS!!!
				// Since RDF properties do not contain the table name
				// The properties query can give me the same property name
				// that appears on 2 different nodes
				String conceptConceptual = Utility.getInstanceName(property);
				if(!conceptConceptual.equals(conceptualName)) {
					continue;
				}
				String propertyConceptual = Utility.getClassName(property);
				qs.addSelector(new QueryColumnSelector(conceptualName + "__" + propertyConceptual));
			}
			
			logger.info("Start node sheet for concept = " + conceptualName);
			IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
			writeNodePropSheet(engine, workbook, iterator, conceptualName, properties);
			logger.info("Finsihed node sheet for concept = " + conceptualName);
		}
		
		// now i need all the relationships
		Vector<String[]> rels = engine.getRelationships(true);
		if(engine.getEngineType() == ENGINE_TYPE.SESAME) {
			for(String[] rel : rels) {
				logger.info("Start rel sheet for " + Arrays.toString(rel));
				String query = generateSparqlQuery(engine, rel[0], rel[1], rel[2]);
				IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, query);
				writeRelationshipSheet(engine, workbook, iterator, rel);
				logger.info("Finsihed rel sheet for " + Arrays.toString(rel));
			}
		} else {
			for(String[] rel : rels) {
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
				qs.setEngine(engine);
				qs.addSelector(new QueryColumnSelector(rel[0]));
				qs.addSelector(new QueryColumnSelector(rel[1]));
				qs.addRelation(rel[0], "inner.join", rel[1]);
				qs.addOrderBy(new QueryColumnOrderBySelector(rel[0]));

				logger.info("Start rel sheet for " + Arrays.toString(rel));
				IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
				writeRelationshipSheet(engine, workbook, iterator, rel);
				logger.info("Finsihed rel sheet for " + Arrays.toString(rel));
			}
		}
		Utility.writeWorkbook(workbook, fileLoc);

		logger.info("Done exporting worksheet for engine = " + engineName);

		String randomKey = UUID.randomUUID().toString();
		this.insight.addExportFile(randomKey, fileLoc);
		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}
	
	public static void writeNodePropSheet(IEngine engine, Workbook workbook, Iterator<IHeadersDataRow> it, String conceptualName, List<String> properties) {
		// write the information for the headers and construct the query
		// so it outputs in the same order
		String physicalNodeName = getPhysicalColumnHeader(engine, conceptualName);
		Sheet sheet = workbook.createSheet(physicalNodeName + "_Props");
				
		// add row 1
		{
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue("Node");
			row.createCell(1).setCellValue(physicalNodeName);
			// add properties
			for(int i = 0; i < properties.size(); i++) {
				String prop = properties.get(i);
				String physicalPropName = getPhysicalColumnHeader(engine, prop);
				row.createCell(2+i).setCellValue(physicalPropName);
			}
		}
		
		// add row 2 - so we can add the ignore
		{
			Row row = sheet.createRow(1);
			row.createCell(0).setCellValue("Ignore");
			if(it.hasNext()) {
				Object[] data = it.next().getValues();
				for(int i = 0 ; i < data.length; i++) {
					if(data[i] instanceof Number) {
						row.createCell(i+1).setCellValue(((Number) data[i]).doubleValue());
					} else {
						row.createCell(i+1).setCellValue(data[i] + "");
					}
				}
			}
		}
		
		// add all other rows
		int rowCounter = 2;
		while(it.hasNext()) {
			Row row = sheet.createRow(rowCounter);
			Object[] data = it.next().getValues();
			for(int i = 0 ; i < data.length; i++) {
				if(data[i] instanceof Number) {
					row.createCell(i+1).setCellValue(((Number) data[i]).doubleValue());
				} else {
					row.createCell(i+1).setCellValue(data[i] + "");
				}			}
			rowCounter++;
		}
	}
	
	public static void writeRelationshipSheet(IEngine engine, Workbook workbook, Iterator<IHeadersDataRow> it, String[] rel) {
		String sheetName = Utility.getInstanceName(rel[0]) + "_" + Utility.getInstanceName(rel[1]) + "_" + Utility.getInstanceName(rel[2]);
		
		Sheet sheet = workbook.createSheet(sheetName);
		
		// add row 1
		{
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue("Relation");
			row.createCell(1).setCellValue(getPhysicalColumnHeader(engine, rel[0]));
			row.createCell(2).setCellValue(getPhysicalColumnHeader(engine, rel[1]));
		}
		// add row 2 - so we can add teh rel name
		{
			Row row = sheet.createRow(1);
			row.createCell(0).setCellValue(rel[2]);
			if(it.hasNext()) {
				Object[] data = it.next().getValues();
				for(int i = 0 ; i < data.length; i++) {
					if(data[i] instanceof Number) {
						row.createCell(i+1).setCellValue(((Number) data[i]).doubleValue());
					} else {
						row.createCell(i+1).setCellValue(data[i] + "");
					}
				}
			}
		}
		// add all other rows
		int rowCounter = 2;
		while(it.hasNext()) {
			Row row = sheet.createRow(rowCounter);
			Object[] data = it.next().getValues();
			for(int i = 0 ; i < data.length; i++) {
				if(data[i] instanceof Number) {
					row.createCell(i+1).setCellValue(((Number) data[i]).doubleValue());
				} else {
					row.createCell(i+1).setCellValue(data[i] + "");
				}
			}
			rowCounter++;
		}
	}
	
	public static String generateSparqlQuery(IEngine engine, String startNode, String endNode, String relName) {
		String query = "select distinct ?start ?end where { "
				+ "{?start a <http://semoss.org/ontologies/Concept/" + getPhysicalColumnHeader(engine, startNode) + ">}"
				+ "{?end a <http://semoss.org/ontologies/Concept/" + getPhysicalColumnHeader(engine, endNode) + ">}"
				+ "{?start <http://semoss.org/ontologies/Relation/" + relName + "> ?end}"
				+ "} order by ?start";
		return query;
	}

	public static String getPhysicalColumnHeader(IEngine engine, String conceptualName) {
		String physicalNodeUri = engine.getPhysicalUriFromConceptualUri(conceptualName);
		String physicalNodeName = Utility.getInstanceName(physicalNodeUri);
		return physicalNodeName;
	}
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("LocalMasterDatabase");
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);

		String testEngine = "MovieDatabase";
		testEngine = "TAP_Core_Data";
		testEngine = "TAP_Site_Data";

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\" + testEngine + ".smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineName(testEngine);
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName(testEngine);
		DIHelper.getInstance().setLocalProperty(testEngine, coreEngine);

		Insight in = new Insight();
		PixelPlanner planner = new PixelPlanner();
		planner.setVarStore(in.getVarStore());
		in.getVarStore().put("$JOB_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
		in.getVarStore().put("$INSIGHT_ID", new NounMetadata("test", PixelDataType.CONST_STRING));
		
		ToLoaderSheetReactor reactor = new ToLoaderSheetReactor();
		reactor.setInsight(in);
		reactor.setPixelPlanner(planner);
		reactor.In();
		reactor.curRow.add(new NounMetadata(testEngine, PixelDataType.CONST_STRING));
		reactor.execute();
	}

}
