package prerna.reactor.frame;

import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetFrameMetamodelReactor extends AbstractFrameReactor {
	
	/*
	 * PAYLOAD MUST MATCH THAT OF 
	 * {@link prerna.sablecc2.reactor.masterdatabase.GetDatabaseMetamodelReactor}
	 */
	
	public GetFrameMetamodelReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		Map<String, Object> metamodelObject = frame.getMetaData().getMetamodel(true);
		return new NounMetadata(metamodelObject, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_METAMODEL);
	}
}
