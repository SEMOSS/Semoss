package prerna.ds.util;

import java.util.Iterator;

import prerna.engine.api.IHeadersDataRow;

public interface IFileIterator extends Iterator<IHeadersDataRow>{

	public enum FILE_DATA_TYPE {STRING, META_DATA_ENUM}
	
}
