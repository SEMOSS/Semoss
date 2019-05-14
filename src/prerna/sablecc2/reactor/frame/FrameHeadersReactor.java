package prerna.sablecc2.reactor.frame;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class FrameHeadersReactor extends AbstractFrameReactor {

	public static final String HEADER_TYPES = "headerTypes";

	public FrameHeadersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), HEADER_TYPES };
	}

	@Override
	public NounMetadata execute() {
		// get the frame
		ITableDataFrame dm = getFrame();
		if (dm == null) {
			NounMetadata noun = new NounMetadata(new HashMap<String, Object>(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS);
			return noun;
		}
		// get the types of the headers requested
		String[] headerTypes = getHeaderTypes();
		NounMetadata noun = new NounMetadata(dm.getFrameHeadersObject(headerTypes), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS);
		return noun;
	}

	/**
	 * Get the types
	 * @return
	 */
	private String[] getHeaderTypes() {
		List<String> retTypes = new Vector<String>();
		GenRowStruct headerTypesGrs = this.store.getNoun(this.keysToGet[1]);
		if(headerTypesGrs != null && !headerTypesGrs.isEmpty()) {
			retTypes = headerTypesGrs.getAllStrValues();
		}
		
		if(retTypes.isEmpty()) {
			retTypes = this.curRow.getAllStrValues();
		}
		
		return retTypes.toArray(new String[]{});
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(HEADER_TYPES)) {
			return "Indicates header data types to be returned";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
