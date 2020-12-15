package prerna.sablecc2.reactor.qs;

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
//			if(frameQs.getSelectors().size() == 0) {
//				if(frame.isEmpty()) {
//					throw new IllegalArgumentException("Frame is empty and contains no data");
//				}
//				throw new IllegalArgumentException("Frame metadata is empty and does not contain any selectors to query");
//			}
			this.qs.setQueryAll(true);
			this.qs.merge(frameQs);
		}
		return this.qs;
	}
	
}
