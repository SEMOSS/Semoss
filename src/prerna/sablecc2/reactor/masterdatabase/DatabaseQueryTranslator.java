package prerna.sablecc2.reactor.masterdatabase;

import java.util.List;
import java.util.Map;

import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.parsers.SqlTranslator;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class DatabaseQueryTranslator extends AbstractReactor {
	public DatabaseQueryTranslator() {
		this.keysToGet = new String[] { "query", "sourceDB", "targetDB" };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String query = this.keyValue.get(this.keysToGet[0]);
		query = Utility.decodeURIComponent(query);
		String sourceDB = this.keyValue.get(this.keysToGet[1]);
		String targetDB = this.keyValue.get(this.keysToGet[2]);
		// get physical to physical translation from sourceDB to targetDB
		// TODO need to figure out how to interpret 1-many maps
		Map<String, List<String>> translation = MasterDatabaseUtility.databaseTranslator(sourceDB, targetDB);
		// process query components using translation map
		SqlTranslator translator = new SqlTranslator(translation);
		SelectQueryStruct translatedQs;
		try {
			translatedQs = translator.processQuery(query);
			IEngine targetEngine = Utility.getEngine(targetDB);
			IQueryInterpreter interpreter = targetEngine.getQueryInterpreter();
			interpreter.setQueryStruct(translatedQs);
			String translatedQuery = interpreter.composeQuery(); 

			// return string query for now
			String colName = "Query";
			String[] types = new String[] { "STRING" };
			String[] headers = new String[] { colName };
			H2Frame frame = new H2Frame(headers);
			String tableName = frame.getTableName();
			String[] cells = new String[] { translatedQuery };
			frame.addRow(tableName, cells, headers, types);
			this.insight.setDataMaker(frame);
			return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		NounMetadata noun = new NounMetadata("Unable to interpret query", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		SemossPixelException exception = new SemossPixelException(noun);
		exception.setContinueThreadOfExecution(false);
		throw exception;

	}
}
