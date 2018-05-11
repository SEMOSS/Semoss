package prerna.sablecc2.reactor.export;

import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import cern.colt.Arrays;
import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.Join;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.sablecc2.reactor.imports.H2Importer;
import prerna.test.TestUtilityMethods;
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
			for(String property : properties) {
				String propertyConceptual = Utility.getClassName(property);
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
			Iterator<IHeadersDataRow> it = dataframe.query("select * from " + dataframe.getTableName());
			while(it.hasNext()) {
				System.out.println(Arrays.toString(it.next().getValues()));
			}
			
			// delete the frame once we are done
			dataframe.dropTable();
			dataframe.dropOnDiskTemporalSchema();
		}
		
		
		return null;
	}
	
	
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper("C:\\workspace\\Semoss_Dev\\RDF_Map.prop");
		
		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\LocalMasterDatabase.smss";
		IEngine coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("LocalMasterDatabase");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("LocalMasterDatabase");
		DIHelper.getInstance().setLocalProperty("LocalMasterDatabase", coreEngine);
		
		engineProp = "C:\\workspace\\Semoss_Dev\\db\\MuleSoft_Speakers.smss";
		coreEngine = new RDBMSNativeEngine();
		coreEngine.setEngineName("MuleSoft_Speakers");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("MuleSoft_Speakers");
		DIHelper.getInstance().setLocalProperty("MuleSoft_Speakers", coreEngine);
		
		ToLoaderSheetReactor reactor = new ToLoaderSheetReactor();
		reactor.In();
		reactor.curRow.add(new NounMetadata("MuleSoft_Speakers", PixelDataType.CONST_STRING));
		reactor.execute();
	}
	

	
	
}
