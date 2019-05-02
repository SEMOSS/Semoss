package prerna.sablecc2.reactor.frame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class FrameHeadersReactor extends AbstractReactor {

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
		
		// get types to include
		Map<String, Object> headersObj = dm.getMetaData().getTableHeaderObjects(headerTypes);
		
		// now loop through and add if there are any filters on the header
		Set<String> filteredCols = dm.getFrameFilters().getAllFilteredColumns();
		List<Map<String, Object>> headersMap = (List<Map<String, Object>>) headersObj.get("headers");
		for(Map<String, Object> headerMap : headersMap) {
			String alias = (String) headerMap.get("alias");
			String rawHeader = (String) headerMap.get("header");
			if(filteredCols.contains(alias) || filteredCols.contains(rawHeader)) {
				headerMap.put("isFiltered", true);
			} else {
				headerMap.put("isFiltered", false);
			}
		}
		
		NounMetadata noun = new NounMetadata(headersObj, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS);
		return noun;
	}

	/**
	 * Getting the frame that is required
	 * @return
	 */
	private ITableDataFrame getFrame() {
		GenRowStruct frameGrs = this.store.getNoun(this.keysToGet[0]);
		if(frameGrs != null && !frameGrs.isEmpty()) {
			return (ITableDataFrame) frameGrs.get(0);
		}
		
		List<Object> frameValues = this.curRow.getValuesOfType(PixelDataType.FRAME);
		if(frameValues != null && !frameValues.isEmpty()) {
			return (ITableDataFrame) frameValues.get(0);
		}
		
		return (ITableDataFrame) this.insight.getDataMaker();
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
