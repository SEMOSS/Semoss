package prerna.ds.util;

import prerna.engine.api.IDatasourceIterator;

public interface IFileIterator extends IDatasourceIterator {

	boolean numberRowsOverLimit(long limitSize);

}
