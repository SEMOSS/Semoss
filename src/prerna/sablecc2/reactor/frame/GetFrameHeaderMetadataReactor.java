package prerna.sablecc2.reactor.frame;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class GetFrameHeaderMetadataReactor extends AbstractReactor {

	public static final String HEADER_TYPES = "headerTypes";

	public GetFrameHeaderMetadataReactor() {
		this.keysToGet = new String[] { HEADER_TYPES };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		ITableDataFrame dm = (ITableDataFrame) this.insight.getDataMaker();
		if (dm == null) {
			NounMetadata noun = new NounMetadata(new HashMap<String, Object>(), PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.FRAME_HEADERS);
			return noun;
		}

		// get types to include
		Map<String, Object> headersObj = null;
		GenRowStruct inputsGRS = this.getCurRow();
		if (inputsGRS != null && !inputsGRS.isEmpty()) {
			// loop if more than one input
			String[] headerTypes = new String[inputsGRS.size()];
			for (int selectIndex = 0; selectIndex < inputsGRS.size(); selectIndex++) {
				String header = getHeader(selectIndex);
				headerTypes[selectIndex] = header;
			}
			headersObj = dm.getMetaData().getTableHeaderObjects(headerTypes);
		} else if (this.keyValue.get(this.keysToGet[0]) != null) {
			// check if data type in key value format
			String header = this.keyValue.get(this.keysToGet[0]);
			String[] headerType = new String[] { header };
			headersObj = dm.getMetaData().getTableHeaderObjects(headerType);
		} else {
			// default to all types
			headersObj = dm.getMetaData().getTableHeaderObjects();
		}
		
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

	private String getHeader(int i) {
		GenRowStruct inputsGRS = this.getCurRow();
		String headerType = inputsGRS.getNoun(i).getValue() + "";
		return headerType;
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
