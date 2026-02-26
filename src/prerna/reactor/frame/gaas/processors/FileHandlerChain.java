package prerna.reactor.frame.gaas.processors;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;

public class FileHandlerChain {
	private final List<IFileHandler> handlerList;

	public FileHandlerChain(Collection<IFileHandler> handlers) {
		if (handlers != null && !handlers.isEmpty()) {
			handlerList = Collections.unmodifiableList(handlers.stream().toList());
		} else {
			throw new IllegalArgumentException("List of handlers can't be empty or null");
		}
	}

	public static FileHandlerChain getCoreHandlerChain() {
		List<IFileHandler> base = new ArrayList<>();
		base.add(new CoreFileHandler());
		return new FileHandlerChain(base);
	}
	
	public FileHandlerChain withHandlers(Collection<? extends IFileHandler> extra) {
	    if (extra == null || extra.isEmpty()) return this;
	    List<IFileHandler> copy = new ArrayList<>(this.handlerList);
	    copy.addAll(extra);
	    return new FileHandlerChain(copy);
	  }

	public int process(File file, VectorDatabaseCSVWriter writer) throws Exception {
		if (writer == null) {
			throw new NullPointerException("VectorDatabaseCSVWriter is not initialized");
		}
		for (IFileHandler handler : handlerList) {
			if (handler.supportsFile(file)) {
				return handler.handleProcessing(file, writer);
			}
		}
		return 0;
	}
	
	public IFileProcessor getFileProcessor(File file, VectorDatabaseCSVWriter writer) {
		IFileProcessor processor = null;
		for (IFileHandler handler : handlerList) {
			processor = handler.getFileProcessor(file, writer);
			if (processor != null) break;
		}
		return processor;
	}

	public boolean supportsFile(File file) {
		boolean supports = false;
		for (IFileHandler handler : handlerList) {
			if (handler.supportsFile(file)) {
				supports = true;
				break;
			}
		}
		return supports;
	}
}