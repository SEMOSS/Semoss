package prerna.reactor.masterdatabase;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.reactor.AbstractReactor;
import prerna.reactor.imports.RdbmsImporter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetPhysicalToLogicalMapping extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		String query = "SELECT e.engineName, ec.physicalName, c.logicalName from Engine e INNER JOIN EngineConcept ec ON e.id=ec.engine INNER JOIN Concept c on ec.localConceptID = c.localConceptID";
		HardSelectQueryStruct qs = new HardSelectQueryStruct();
		qs.setQuery(query);
		qs.setQsType(QUERY_STRUCT_TYPE.RAW_ENGINE_QUERY);
		qs.setEngine(Utility.getDatabase(Constants.LOCAL_MASTER_DB));
		
		H2Frame frame = new H2Frame();
		RdbmsImporter importer = new RdbmsImporter(frame, qs);
		importer.insertData();
		this.insight.setDataMaker(frame);
		return new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME);
	}

}
