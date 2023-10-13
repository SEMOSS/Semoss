package prerna.reactor.frame;

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

	private static final String HEADER_TYPES = "headerTypes";
	private static final String RESET = "reset";
	
	public FrameHeadersReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.FRAME.getKey(), HEADER_TYPES, RESET};
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
		PixelOperationType[] opTypes = null;
		if(reset()) {
			opTypes = new PixelOperationType[] {PixelOperationType.FRAME_HEADERS, PixelOperationType.FRAME_HEADERS_CHANGE};
		} else {
			opTypes = new PixelOperationType[] {PixelOperationType.FRAME_HEADERS};
		}
		NounMetadata noun = new NounMetadata(dm.getFrameHeadersObject(headerTypes), PixelDataType.CUSTOM_DATA_STRUCTURE, opTypes);
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
		
		return retTypes.toArray(new String[retTypes.size()]);
	}
	
	private boolean reset() {
		GenRowStruct resetGrs = this.store.getNoun(this.keysToGet[2]);
		if(resetGrs != null && !resetGrs.isEmpty()) {
			NounMetadata n = resetGrs.getNoun(0);
			if(n.getNounType() == PixelDataType.BOOLEAN) {
				return (boolean) n.getValue();
			} else {
				return Boolean.parseBoolean(n.getValue()+"");
			}
		}
		
		// see if the curRow has it
		List<Object> resetNoun = this.curRow.getValuesOfType(PixelDataType.BOOLEAN);
		if(resetNoun != null && !resetNoun.isEmpty()) {
			return (boolean) resetNoun.get(0);
		}
		
		// default is false
		return false;
	}
	

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(HEADER_TYPES)) {
			return "Indicates header data types to be returned";
		} else if (key.equals(RESET)) {
			return "Boolean to tell the UI to reset the frame headers currently exposed";
		} else {
			return super.getDescriptionForKey(key);
		}
	}

}
