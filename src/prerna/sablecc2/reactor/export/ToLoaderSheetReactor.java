package prerna.sablecc2.reactor.export;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.ENGINE_TYPE;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportFactory;
import prerna.test.TestUtilityMethods;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ToLoaderSheetReactor extends AbstractReactor {

	public ToLoaderSheetReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.DATABASE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineName = this.keyValue.get(this.keysToGet[0]);
		if(engineName == null) {
			throw new IllegalArgumentException("Must define which database to get a loader sheet from");
		}

		String fileLoc = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "\\" + engineName + "_Loader_Sheet_Export.xlsx";
		Workbook workbook = new XSSFWorkbook();
		
		IEngine engine = Utility.getEngine(engineName);
		// get a list of all the tables and properties
		Vector<String> concepts = engine.getConcepts(true);

		for(String concept : concepts) {
			if(concept.equals("http://semoss.org/ontologies/Concept")) {
				continue;
			}
			// we will create a frame
			// and merge every property onto it
			// using a left join
			ITableDataFrame dataframe = new H2Frame();
			String conceptualName = Utility.getInstanceName(concept);
			// first add the concept by itself
			{
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
				qs.setEngine(engine);
				qs.addSelector(new QueryColumnSelector(conceptualName));
				IImporter importer = ImportFactory.getImporter(dataframe, qs);
				importer.insertData();
				dataframe.syncHeaders();
			}

			List<String> properties = engine.getProperties4Concept(concept, true);
			List<String> propertyConceptualNames = new Vector<String>(properties.size());
			for(String property : properties) {
				String propertyConceptual = Utility.getClassName(property);
				propertyConceptualNames.add(propertyConceptual);
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
				qs.setEngine(engine);
				qs.addSelector(new QueryColumnSelector(conceptualName));
				qs.addSelector(new QueryColumnSelector(conceptualName + "__" + propertyConceptual));
				IImporter importer = ImportFactory.getImporter(dataframe, qs);
				List<Join> joins = new Vector<Join>();
				Join j = new Join(conceptualName, "left.outer.join", conceptualName);
				joins.add(j);
				importer.mergeData(joins);
				dataframe.syncHeaders();
			}

			// once i am done adding all the data
			// write the h2frame to the excel sheet
			writeNodePropSheet(engine, workbook, dataframe, conceptualName, propertyConceptualNames);
			
			// delete the frame once we are done
			if(dataframe instanceof H2Frame) {
				((H2Frame) dataframe).dropTable();
				((H2Frame) dataframe).dropOnDiskTemporalSchema();
			}
		}
		
		// now i need all the relationships
		Vector<String[]> rels = engine.getRelationships(true);
		if(engine.getEngineType() == ENGINE_TYPE.SESAME) {
			for(String[] rel : rels) {
				String query = generateSparqlQuery(engine, rel[0], rel[1], rel[2]);
				IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, query);
				writeRelationshipSheet(engine, workbook, iterator, rel);
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

				IRawSelectWrapper iterator = WrapperManager.getInstance().getRawWrapper(engine, qs);
				writeRelationshipSheet(engine, workbook, iterator, rel);
			}
		}
		Utility.writeWorkbook(workbook, fileLoc);

		System.out.println("done");
		return null;
	}
	
	public static void writeRelationshipSheet(IEngine engine, Workbook workbook, Iterator<IHeadersDataRow> it, String[] rel) {
		String sheetName = rel[0] + "_" + rel[1] + "_" + rel[2];
		
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
					row.createCell(i+1).setCellValue(data[i] + "");
				}
			}
		}
		// add all other rows
		int rowCounter = 2;
		while(it.hasNext()) {
			Row row = sheet.createRow(rowCounter);
			Object[] data = it.next().getValues();
			for(int i = 0 ; i < data.length; i++) {
				row.createCell(i+1).setCellValue(data[i] + "");
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

	public static void writeNodePropSheet(IEngine engine, Workbook workbook, ITableDataFrame frame, String conceptualName, List<String> properties) {
		// write the information for the headers and construct the query
		// so it outputs in the same order
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.setDistinct(false);
		String physicalNodeName = getPhysicalColumnHeader(engine, conceptualName);
		Sheet sheet = workbook.createSheet(physicalNodeName + "_Props");

		{
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue("Node");
			// add conceptual name
			qs.addSelector(new QueryColumnSelector(conceptualName));
			row.createCell(1).setCellValue(physicalNodeName);
			// add properties
			for(int i = 0; i < properties.size(); i++) {
				String prop = properties.get(i);
				String physicalPropName = getPhysicalColumnHeader(engine, prop);
				qs.addSelector(new QueryColumnSelector(prop));
				row.createCell(2+i).setCellValue(physicalPropName);
			}
			
			qs.addOrderBy(new QueryColumnOrderBySelector(conceptualName));
		}

		int rowCounter = 1;
		Iterator<IHeadersDataRow> it = frame.query(qs);
		while(it.hasNext()) {
			IHeadersDataRow rowValues = it.next();
			Object[] values = rowValues.getValues();

			Row row = sheet.createRow(rowCounter);
			
			// add the dumb ignore cell
			if(rowCounter == 1) {
				row.createCell(0).setCellValue("Ignore");
			}
			
			for(int i = 0; i < values.length; i++) {
				Cell cell = row.createCell(i+1);
				if(values[i] != null) {
					cell.setCellValue(values[i].toString());
				}
			}

			rowCounter++;
		}
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
		testEngine = "TAP_Site_Data";
		
		engineProp = "C:\\workspace\\Semoss_Dev\\db\\" + testEngine + ".smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineName(testEngine);
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName(testEngine);
		DIHelper.getInstance().setLocalProperty(testEngine, coreEngine);

		ToLoaderSheetReactor reactor = new ToLoaderSheetReactor();
		reactor.In();
		reactor.curRow.add(new NounMetadata(testEngine, PixelDataType.CONST_STRING));
		reactor.execute();
	}




}
