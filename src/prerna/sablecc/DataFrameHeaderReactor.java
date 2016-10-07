package prerna.sablecc;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.algorithm.api.ITableDataFrame;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.DataframeHeaderMetadata;
import prerna.sablecc.meta.DataframeMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class DataFrameHeaderReactor extends AbstractReactor {

	public DataFrameHeaderReactor() {
		String[] thisReacts = {};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_FRAME_HEADER;
	}

	@Override
	public Iterator process() {
		ITableDataFrame table = (ITableDataFrame) myStore.get("G");
		myStore.put("tableHeaders", table.getTableHeaderObjects());
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
