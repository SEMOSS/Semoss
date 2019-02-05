package prerna.sablecc2.om.task;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import prerna.engine.api.IHeadersDataRow;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.sablecc2.om.task.options.TaskOptions;
import prerna.sablecc2.reactor.export.Formatter;

public interface ITask extends Iterator<IHeadersDataRow> {

	/**
	 * Basic operation to get a certain number of records from the data
	 * Meta is used to determine if we need to send additional meta data
	 * around the creation of the task
	 * @param numRecordsToGet
	 * @param meta
	 * @return
	 */
	Map<String, Object> collect(boolean meta);
	
	Map<String, Object> getMetaMap();
	
	boolean getMeta();
	
	void setMeta(boolean meta);
	
	void setNumCollect(int numCollect);
	
	int getNumCollect();
	
	void setId(String taskId);
	
	String getId();

	void setFormat(String formatType);

	Formatter getFormatter();

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
	
	void cleanUp();
	
	void setLogger(Logger logger);
	
	void optimizeQuery(int limit);
	
	void reset();

}
