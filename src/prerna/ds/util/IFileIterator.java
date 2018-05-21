package prerna.ds.util;

import java.util.Iterator;

import prerna.algorithm.api.SemossDataType;
import prerna.engine.api.IHeadersDataRow;

public interface IFileIterator extends Iterator<IHeadersDataRow> {

	String[] getHeaders();

	SemossDataType[] getTypes();

	boolean numberRowsOverLimit(int limitSize);

	int getNumRecords();
}
