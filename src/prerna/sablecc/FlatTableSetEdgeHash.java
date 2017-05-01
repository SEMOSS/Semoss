package prerna.sablecc;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import prerna.ds.AbstractTableDataFrame;
import prerna.ds.DataFrameHelper;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.sablecc.meta.DataframeSetEdgeHashMetadata;
import prerna.sablecc.meta.IPkqlMetadata;

public class FlatTableSetEdgeHash extends AbstractReactor {

	private Map<String, Set<String>> newEdgeHash;
	
	@Override
	public IPkqlMetadata getPkqlMetadata() {
		return new DataframeSetEdgeHashMetadata(this.newEdgeHash);
	}

	@Override
	public Iterator process() {
		AbstractTableDataFrame frame = (AbstractTableDataFrame) myStore.get("G");
		
		// column name and new type are passed in the pkql
		String newEdgeHashStr = (String) myStore.get(PKQLEnum.WORD_OR_NUM);
		this.newEdgeHash = DataFrameHelper.generateEdgeHashFromStr(newEdgeHashStr);
		frame.recreateMetadata(newEdgeHash);
		
		myStore.put("STATUS", STATUS.SUCCESS);
		return null;
	}

}
