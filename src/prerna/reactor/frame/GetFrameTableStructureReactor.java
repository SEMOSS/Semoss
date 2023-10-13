package prerna.reactor.frame;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetFrameTableStructureReactor extends AbstractFrameReactor {
	
	/*
	 * PAYLOAD MUST MATCH THAT OF 
	 * {@link prerna.sablecc2.reactor.masterdatabase.GetDatabaseTableStructureReactor}
	 */
	
	private static final String CLASS_NAME = GetFrameTableStructureReactor.class.getName();
	
	public GetFrameTableStructureReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		List<Object[]> data = frame.getMetaData().getAllTablesAndColumns();
		return new NounMetadata(data, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_TABLE_STRUCTURE);
	}
}
