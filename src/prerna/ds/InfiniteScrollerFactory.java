package prerna.ds;

import java.util.List;

import prerna.algorithm.api.ITableDataFrame;

public class InfiniteScrollerFactory {

	public static InfiniteScroller getInfiniteScroller(ITableDataFrame table) {
		if(table instanceof BTreeDataFrame) {
			return new BTreeInfiniteScroller((BTreeDataFrame)table);
		} else {
			throw new IllegalArgumentException("ITableDataFrame instance not supported");
		}
	}
	
	public static InfiniteScroller getInfiniteScroller(List<String> instances) {
		return null;
	}
}
