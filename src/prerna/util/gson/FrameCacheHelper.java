package prerna.util.gson;

import java.util.List;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;

public class FrameCacheHelper {
	
	/**
	 * Simple object to help cache frames in Insight Caching
	 */
	
	private ITableDataFrame frame;
	private List<String> alias = new Vector<String>();
	
	FrameCacheHelper(ITableDataFrame frame) {
		this.frame = frame;
	}
	
	public void addAlias(String alias) {
		this.alias.add(alias);
	}
	
	public boolean sameFrame(ITableDataFrame frame) {
		return this.frame == frame;
	}
	
	public ITableDataFrame getFrame() {
		return this.frame;
	}
	
	public List<String> getAlias() {
		return this.alias;
	}
}