package prerna.sablecc;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.DataframeHeaderMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class DataFrameHeaderReactor extends AbstractReactor {

	public static final String ADDITIONAL_INFO_BOOL = "additionalInfoBool";
	
	public DataFrameHeaderReactor() {
		String[] thisReacts = {};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_FRAME_HEADER;
	}

	@Override
	public Iterator process() {
		ITableDataFrame table = (ITableDataFrame) myStore.get("G");
		Boolean includeAdditionalInfo = (Boolean) myStore.get(ADDITIONAL_INFO_BOOL);
		if(!(includeAdditionalInfo == null || includeAdditionalInfo.equals(false))) {
			Set<String> orderHeaders = new TreeSet<String>();
			orderHeaders.addAll(Arrays.asList(table.getColumnHeaders()));
			Map<String, Set<String>> retMap = new Hashtable<String, Set<String>>();
			retMap.put("list", orderHeaders);
			myStore.put("tableHeaders", retMap);
		} else {
			List<Map<String, Object>> tableHeaderObjects = table.getTableHeaderObjects();
			for(Map<String, Object> nextObject : tableHeaderObjects) {
				Object name = nextObject.remove("varKey");
				nextObject.remove("uri");
				nextObject.put("name", name);
			}
			
			myStore.put("tableHeaders", tableHeaderObjects);
		}
		myStore.put("RESPONSE", STATUS.SUCCESS.toString());
		myStore.put("STATUS", STATUS.SUCCESS);

		return null;
	}

	public IPkqlMetadata getPkqlMetadata() {
		DataframeHeaderMetadata metadata = new DataframeHeaderMetadata();
		metadata.setPkqlStr((String) myStore.get(PKQLEnum.DATA_FRAME_HEADER));
		return metadata;
	}

}
