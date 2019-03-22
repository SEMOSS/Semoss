package prerna.sablecc2.reactor.export;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
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
		String app = this.keyValue.get(this.keysToGet[0]);
		if(app == null) {
			throw new IllegalArgumentException("Need to specify the app to export");
		}
		String appId = MasterDatabaseUtility.testEngineIdIfAlias(app);
		IEngine engine = Utility.getEngine(appId);
		if(engine == null) {
			throw new IllegalArgumentException("Cannot find the specified app");
		}
		String propFileLoc = DIHelper.getInstance().getProperty(appId + "_" + Constants.STORE);
		Properties prop = Utility.loadProperties(propFileLoc);
		
		Date date = new Date();
		String modifiedDate = new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(date);
		String fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\" + SmssUtilities.getUniqueName(prop) + "_" + modifiedDate + "_Loader_Sheet_Export.xlsx";
		File f = new File(fileLoc);
		if(f.exists()) {
			f.delete();
		}
		Workbook workbook = new XSSFWorkbook();
		
		// get a list of all the tables and properties
		Vector<String> concepts = engine.getConcepts(false);

		for(String conceptPhysicalUri : concepts) {
			if(conceptPhysicalUri.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			String physicalConceptName = Utility.getInstanceName(conceptPhysicalUri);
			String conceptConceptualUri = engine.getConceptualUriFromPhysicalUri(conceptPhysicalUri);
			if(conceptConceptualUri == null) {
				// this is most likely because you have a weird subclass you added
				// but didn't give it a proper conceptual name
				// like ActiveSystem
				continue;
			}
			String conceptualName = Utility.getInstanceName(conceptConceptualUri);
			
			SelectQueryStruct qs = new SelectQueryStruct();
			qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
			qs.setEngine(engine);
			qs.addSelector(new QueryColumnSelector(conceptualName));
			
			List<String> properties = engine.getProperties4Concept(conceptPhysicalUri, false);
			for(int i = 0; i < properties.size(); i++) {
				String propertyPhysicalUri = properties.get(i);
				String propertyConceptualUri = engine.getConceptualUriFromPhysicalUri(propertyPhysicalUri);
				String propertyConceptualName = Utility.getClassName(propertyConceptualUri);
				qs.addSelector(new QueryColumnSelector(conceptualName + "__" + propertyConceptualName));
			}
			
			logger.info("Start node sheet for concept = " + conceptualName);
			IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
			writeNodePropSheet(engine, workbook, iterator, physicalConceptName, properties);
			logger.info("Finsihed node sheet for concept = " + conceptualName);
		}
		
		// now i need all the relationships
		Vector<String[]> rels = engine.getRelationships(true);
		if(engine.getEngineType() == ENGINE_TYPE.SESAME) {
			for(String[] rel : rels) {
				logger.info("Start rel sheet for " + Arrays.toString(rel));
				List<String> edgeProps = getEdgeProperties(engine, rel[0], rel[1], rel[2]);
				String query = generateSparqlQuery(engine, rel[0], rel[1], rel[2], edgeProps);
				IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, query);
				writeRelationshipSheet(engine, workbook, iterator, rel, edgeProps);
				logger.info("Finsihed rel sheet for " + Arrays.toString(rel));
			}
		} else {
			for(String[] rel : rels) {
				String toConceptualName = Utility.getInstanceName(rel[0]);
				String fromConceptualName = Utility.getInstanceName(rel[1]);

				SelectQueryStruct qs = new SelectQueryStruct();
				qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
				qs.setEngine(engine);
				qs.addSelector(new QueryColumnSelector(toConceptualName));
				qs.addSelector(new QueryColumnSelector(fromConceptualName));
				qs.addRelation(toConceptualName, fromConceptualName, "inner.join");
				qs.addOrderBy(new QueryColumnOrderBySelector(toConceptualName));

				logger.info("Start rel sheet for " + Arrays.toString(new String[] {toConceptualName, fromConceptualName}));
				IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
				writeRelationshipSheet(engine, workbook, iterator, rel, new ArrayList<String>());
				logger.info("Finsihed rel sheet for " + Arrays.toString(rel));
			}
		}
		
		logger.info("Start writing loader sheet");
		writeLoader(workbook);
		logger.info("Finsihed Writing loader sheet");

		logger.info("Start exporting");
		Utility.writeWorkbook(workbook, fileLoc);
		logger.info("Done exporting worksheet for engine = " + app);

		String randomKey = UUID.randomUUID().toString();
		this.insight.addExportFile(randomKey, fileLoc);
		return new NounMetadata(randomKey, PixelDataType.CONST_STRING, PixelOperationType.FILE_DOWNLOAD);
	}
	
	public static void writeNodePropSheet(IEngine engine, Workbook workbook, Iterator<IHeadersDataRow> it, String physicalNodeName, List<String> properties) {
		// write the information for the headers and construct the query
		// so it outputs in the same order
		boolean isRdbms = engine.getEngineType() == ENGINE_TYPE.IMPALA || engine.getEngineType() == ENGINE_TYPE.RDBMS;
		Sheet sheet = workbook.createSheet(physicalNodeName + "_Props");
				
		// add row 1
		{
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue("Node");
			row.createCell(1).setCellValue(physicalNodeName);
			// add properties
			for (int i = 0; i < properties.size(); i++) {
				String prop = properties.get(i);
				String physicalPropName = null;
				if(isRdbms) {
					physicalPropName = Utility.getClassName(prop);
				} else {
					physicalPropName = Utility.getInstanceName(prop);
				}
				row.createCell(2 + i).setCellValue(physicalPropName);
			}
		}
		
		// add row 2 - so we can add the ignore
		{
			Row row = sheet.createRow(1);
			row.createCell(0).setCellValue("Ignore");
			if(it.hasNext()) {
				Object[] data = it.next().getValues();
				for(int i = 0 ; i < data.length; i++) {
					if(data[i] == null) {
						row.createCell(i+1).setCellType(CellType.BLANK);
					} else if(data[i] instanceof Number) {
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
				if(data[i] == null) {
					row.createCell(i+1).setCellType(CellType.BLANK);
				} else if(data[i] instanceof Number) {
					row.createCell(i+1).setCellValue(((Number) data[i]).doubleValue());
				} else {
					row.createCell(i+1).setCellValue(data[i] + "");
				}			
			}
			rowCounter++;
		}
	}
	
	public static void writeRelationshipSheet(IEngine engine, Workbook workbook, Iterator<IHeadersDataRow> it, String[] rel, List<String> edgeProps) {
		String sheetName = Utility.getInstanceName(rel[0]) + "_" + Utility.getInstanceName(rel[1]) + "_" + Utility.getInstanceName(rel[2]);
		
		Sheet sheet = workbook.createSheet(sheetName);
		
		// add row 1
		{
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue("Relation");
			row.createCell(1).setCellValue(getPhysicalColumnHeader(engine, rel[0]));
			row.createCell(2).setCellValue(getPhysicalColumnHeader(engine, rel[1]));
			if(edgeProps != null) {
				for(int i = 0; i < edgeProps.size(); i++) {
					row.createCell(3+i).setCellValue(edgeProps.get(i));
				}
			}
		}
		// add row 2 - so we can add teh rel name
		{
			Row row = sheet.createRow(1);
			row.createCell(0).setCellValue(Utility.getInstanceName(rel[2]));
			if(it.hasNext()) {
				Object[] data = it.next().getValues();
				for(int i = 0 ; i < data.length; i++) {
					if(data[i] == null) {
						row.createCell(i+1).setCellType(CellType.BLANK);
					} else if(data[i] instanceof Number) {
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
				if(data[i] == null) {
					row.createCell(i+1).setCellType(CellType.BLANK);
				} else if(data[i] instanceof Number) {
					row.createCell(i+1).setCellValue(((Number) data[i]).doubleValue());
				} else {
					row.createCell(i+1).setCellValue(data[i] + "");
				}
			}
			rowCounter++;
		}
	}
	
	public static void writeLoader(Workbook wb) {
		int numSheets = wb.getNumberOfSheets();
		List<String> sheetNames = new Vector<String>(numSheets);
		for(int i = 0; i < numSheets; i++) {
			sheetNames.add(wb.getSheetName(i));
		}
		
		Sheet loader = wb.createSheet("Loader");
		{
			Row headerRow = loader.createRow(0);
			headerRow.createCell(0).setCellValue("Sheet");
			headerRow.createCell(1).setCellValue("Type");
		}
		
		for(int i = 0; i < numSheets; i++) {
			Row sheetRow = loader.createRow(i+1);
			sheetRow.createCell(0).setCellValue(sheetNames.get(i));
			sheetRow.createCell(1).setCellValue("Usual");
		}
		
		// the loader sheet first
		wb.setSheetOrder("Loader", 0);
	}
	
	public static String generateSparqlQuery(IEngine engine, String startNode, String endNode, String relName, List<String> edgeProps) {
		String query = null;
		if(edgeProps.isEmpty()) {
			query = "select distinct ?start ?end where { "
				+ "{?start a <http://semoss.org/ontologies/Concept/" + getPhysicalColumnHeader(engine, startNode) + ">}"
				+ "{?end a <http://semoss.org/ontologies/Concept/" + getPhysicalColumnHeader(engine, endNode) + ">}"
				+ "{?start <http://semoss.org/ontologies/Relation/" + Utility.getInstanceName(relName) + "> ?end}"
				+ "} order by ?start";
		} else {
			StringBuilder b = new StringBuilder();
			b.append("select distinct ?start ?end ");
			for(int i = 0; i < edgeProps.size(); i++) {
				b.append("?prop").append(i).append(" ");
			}
			b.append("where { "
					+ "{?start a <http://semoss.org/ontologies/Concept/" + getPhysicalColumnHeader(engine, startNode) + ">}"
					+ "{?end a <http://semoss.org/ontologies/Concept/" + getPhysicalColumnHeader(engine, endNode) + ">}"
					+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/" + Utility.getInstanceName(relName) + ">}"
					+ "{?start ?rel ?end}");
			for(int i = 0; i < edgeProps.size(); i++) {
				b.append("OPTIONAL{?rel <http://semoss.org/ontologies/Relation/Contains/").append(edgeProps.get(i)).append("> ?prop").append(i).append("}");
			}
			b.append("} order by ?start");
			query = b.toString();
		}
		return query;
	}
	
	public static List<String> getEdgeProperties(IEngine engine, String startNode, String endNode, String relName) {
		String startQ = "select distinct ?propUri where { "
				+ "{?start a <http://semoss.org/ontologies/Concept/" + getPhysicalColumnHeader(engine, startNode) + ">}"
				+ "{?end a <http://semoss.org/ontologies/Concept/" + getPhysicalColumnHeader(engine, endNode) + ">}"
				+ "{?rel <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/" + Utility.getInstanceName(relName) + ">}"
				+ "{?start ?rel ?end}"
				+ "{?propUri a <http://semoss.org/ontologies/Relation/Contains>}"
				+ "{?rel ?propUri ?prop}"
				+ "} order by ?propUri";
		
		List<String> props = new Vector<String>();
		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(engine, startQ);
		while(it.hasNext()) {
			props.add(it.next().getValues()[0].toString());
		}
		return props;
	}

	public static String getPhysicalColumnHeader(IEngine engine, String conceptualName) {
		String physicalNodeUri = engine.getConceptPhysicalUriFromConceptualUri(conceptualName);
		String physicalNodeName = Utility.getInstanceName(physicalNodeUri);
		return physicalNodeName;
	}
	
	public static void main(String[] args) throws Exception {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineId("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineId("LocalMasterDatabase");
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);

		String testEngine = "MovieDatabase";
		testEngine = "TAP_Core_Data";
//		testEngine = "TAP_Site_Data";

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\" + testEngine + ".smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineId(testEngine);
		coreEngine.openDB(engineProp);
		coreEngine.setEngineId(testEngine);
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
