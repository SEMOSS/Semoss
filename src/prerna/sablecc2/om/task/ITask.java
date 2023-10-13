package prerna.sablecc2.om.task;

import java.io.Closeable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import prerna.ds.shared.RawCachedWrapper;
import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.export.IFormatter;
import prerna.sablecc2.om.task.options.TaskOptions;

public interface ITask extends Iterator<IHeadersDataRow>, Closeable {

	/**
	 * Basic operation to get a certain number of records from the data
	 * Meta is used to determine if we need to send additional meta data
	 * around the creation of the task
	 * @param numRecordsToGet
	 * @param meta
	 * @return
	 * @throws Exception 
	 */
	Map<String, Object> collect(boolean meta) throws Exception;
	
	Map<String, Object> getMetaMap();
	
	boolean getMeta();
	
	void setMeta(boolean meta);
	
	void setNumCollect(int numCollect);
	
	int getNumCollect();
	
	void setId(String taskId);
	
	String getId();

	void setFormat(String formatType);
	
	void setFormat(IFormatter formatter);

	IFormatter getFormatter();

	void setFormatOptions(Map<String, Object> optionValues);

	void setTaskOptions(TaskOptions taskOptions);

	TaskOptions getTaskOptions();
	
	void setHeaderInfo(List<Map<String, Object>> headerInfo);

	List<Map<String, Object>> getHeaderInfo();

	void setSortInfo(List<Map<String, Object>> sortInfo);

	List<Map<String, Object>> getSortInfo();
	
	void setFilterInfo(GenRowFilters grf);

	List<Map<String, Object>> getFilterInfo();

	List<Object[]> flushOutIteratorAsGrid();
	
	void setLogger(Logger logger);
	
	void optimizeQuery(int limit) throws Exception;
	
	boolean isOptimized();
	
	void toOptimize(boolean toOptimize);
	
	void reset() throws Exception;
	
	// creates a cache object to be utilized
	RawCachedWrapper createCache() throws Exception;
	
	// get the pragma being set
	String getPragma(String key);
	
}
