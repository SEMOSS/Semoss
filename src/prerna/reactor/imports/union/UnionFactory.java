package prerna.reactor.imports.union;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;

/**
 * A factory class to dispense union routines
 * based on the frame type.
 *
 */

public abstract class UnionFactory {
	
	public static UnionRoutine getUnionRoutine(ITableDataFrame frame) {
		if(frame instanceof RDataTable) {
			return new RUnion();
		}else if(frame instanceof PandasFrame) {
			return new PyUnion();
		}else 
			throw new IllegalArgumentException("This frame type is not supported for union as of now. "
					+ "Please convert frame to R or Python frame.");
		
	}

}
