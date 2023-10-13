package prerna.reactor.qs;

import prerna.algorithm.api.ITableDataFrame;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;

public class QueryAllReactor extends AbstractQueryStructReactor {
	
	@Override
	protected AbstractQueryStruct createQueryStruct() {
		ITableDataFrame frame = this.qs.getFrame();
		if(frame == null) {
			frame = (ITableDataFrame) this.insight.getDataMaker();
			this.qs.setFrame(frame);
		}
		if(frame != null) {
			SelectQueryStruct frameQs = frame.getMetaData().getFlatTableQs(true);
			this.qs.setQueryAll(true);
			this.qs.merge(frameQs);
			this.qs.setQsType(SelectQueryStruct.QUERY_STRUCT_TYPE.FRAME);
		}
		return this.qs;
	}
	
}
