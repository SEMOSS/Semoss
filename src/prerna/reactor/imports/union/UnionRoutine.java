package prerna.reactor.imports.union;

import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.om.Insight;

/**
 * Contract for all concrete union routine classes.
 *
 */

public interface UnionRoutine {
	
	ITableDataFrame performUnion(ITableDataFrame a, ITableDataFrame b, String unionType, Insight insight, Logger logger);
	
	void setColMapping(Map<String, String> cols);
	
}
