package prerna.reactor.imports.union;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;

public abstract class AbstractUnion implements UnionRoutine{
	
	/*
	 * TODO: Below methods needs to be generalised for all
	 * frame types. 
	 */
	
	/**
	 * Check all the base cases for the Union to occur.
	 * 
	 * @param frameA
	 * @param frameB
	 */
	
	protected void checkRBaseCases(ITableDataFrame frameA, ITableDataFrame frameB, List<String> aCols, List<String> bCols) {
		boolean frameAType = frameA instanceof RDataTable;
		boolean frameBType = frameB instanceof RDataTable;
		//boolean isLenSame = aCols.size() == bCols.size();
		
		if(!frameAType)
			throw new IllegalArgumentException("Please convert " + frameA.getName() + " to a R frame before union.");
		if(!frameBType)
			throw new IllegalArgumentException("Please convert " + frameB.getName() + " to a R frame before union.");
//		if(!isLenSame)
//			throw new IllegalArgumentException("Number of columns not same in both the datasets. Please ensure that both the datasets have the same number of columns.");
		checkColNames(aCols, bCols);	
	}
	
	/**
	 * Check all the base cases for the Union to occur on Py frames.
	 * 
	 * @param frameA
	 * @param frameB
	 */
	protected void checkPyBaseCases(ITableDataFrame frameA, ITableDataFrame frameB, List<String> aCols, List<String> bCols) {
		boolean frameAType = frameA instanceof PandasFrame;
		boolean frameBType = frameB instanceof PandasFrame;
		//boolean isLenSame = aCols.size() == bCols.size();
		
		if(!frameAType)
			throw new IllegalArgumentException("Please convert " + frameA.getName() + " to a Py frame before union.");
		if(!frameBType)
			throw new IllegalArgumentException("Please convert " + frameB.getName() + " to a Py frame before union.");
//		if(!isLenSame)
//			throw new IllegalArgumentException("Number of columns not same in both the datasets. Please ensure that both the datasets have the same number of columns.");
		checkColNames(aCols, bCols);
	}
	
	public void deleteFrameCols(ITableDataFrame frame, String col) {
		frame.removeColumn(col);
	}
	
	public void checkColNames(List<String> aCols, List<String> bCols) {
		//Set<String> colsA = new HashSet<>(aCols);
		Set<String> colsB = new HashSet<>(bCols);
		for(String col : aCols) {
			if(!colsB.contains(col)) {
				throw new IllegalArgumentException("Mismatch detected for column " + col + " Please make sure col " + col + " is present in both the datasets.");
			}
		}
	}
	
	public List<String> getSemossCols(String[] cols){
		List<String> listOfCols = new ArrayList<>();
		for(String col : cols) {
			listOfCols.add(col.split("__")[1]);
		}
		return listOfCols;
	}

}
