package prerna.sablecc2.reactor.export;

import java.util.Arrays;
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
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.imports.H2Importer;
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
			H2Frame dataframe = new H2Frame();
			String conceptualName = Utility.getInstanceName(concept);
			// first add the concept by itself
			{
				SelectQueryStruct qs = new SelectQueryStruct();
				qs.setQsType(QUERY_STRUCT_TYPE.ENGINE);
				qs.setEngine(engine);
				qs.addSelector(new QueryColumnSelector(conceptualName));
				H2Importer importer = new H2Importer(dataframe, qs);
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
				H2Importer importer = new H2Importer(dataframe, qs);
				List<Join> joins = new Vector<Join>();
				Join j = new Join(conceptualName, "left.outer.join", conceptualName);
				joins.add(j);
				importer.mergeData(joins);
				dataframe.syncHeaders();
			}

			// once i am done adding all the data
			// write the h2frame to the excel sheet
			writeNodePropSheet(workbook, dataframe, conceptualName, propertyConceptualNames);
			
			// delete the frame once we are done
			dataframe.dropTable();
			dataframe.dropOnDiskTemporalSchema();
		}
		
		
		// now i need all the relationships
		Vector<String[]> rels = engine.getRelationships();
		for(String[] rel : rels) {
			H2Frame dataframe = new H2Frame();
			
			System.out.println(Arrays.toString(rel));
			
			
			
			// delete the frame once we are done
			dataframe.dropTable();
			dataframe.dropOnDiskTemporalSchema();
		}
		
		Utility.writeWorkbook(workbook, fileLoc);

		System.out.println("done");
		return null;
	}


	public static void writeNodePropSheet(Workbook workbook, ITableDataFrame frame, String conceptualName, List<String> properties) {
		// write the information for the headers and construct the query
		// so it outputs in the same order
		SelectQueryStruct qs = new SelectQueryStruct();
		Sheet sheet = workbook.createSheet(conceptualName + "_Props");

		{
			Row row = sheet.createRow(0);
			row.createCell(0).setCellValue("Node");
			// add conceptual name
			qs.addSelector(new QueryColumnSelector(conceptualName));
			row.createCell(1).setCellValue(conceptualName);
			// add properties
			for(int i = 0; i < properties.size(); i++) {
				String prop = properties.get(i);
				qs.addSelector(new QueryColumnSelector(prop));
				row.createCell(2+i).setCellValue(prop);
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


	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("LocalMasterDatabase");
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Site_Data.smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineName("TAP_Site_Data");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("TAP_Site_Data");
		DIHelper.getInstance().setLocalProperty("TAP_Site_Data", coreEngine);

		ToLoaderSheetReactor reactor = new ToLoaderSheetReactor();
		reactor.In();
		reactor.curRow.add(new NounMetadata("TAP_Site_Data", PixelDataType.CONST_STRING));
		reactor.execute();
	}




}
