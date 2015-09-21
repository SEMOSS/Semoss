package prerna.ds;

import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;

/**
 * 
 * @author rluthar
 *
 */
public class InfiniteScrollerFactory {

	public static InfiniteScroller getInfiniteScroller(ITableDataFrame table) {
		if(table instanceof BTreeDataFrame) {
			return new BTreeInfiniteScroller((BTreeDataFrame)table);
		} else {
			throw new IllegalArgumentException("ITableDataFrame instance not supported");
		}
	}
	
	public static InfiniteScroller getInfiniteScroller(Set<String> instances) {
		return null;
	}
}
