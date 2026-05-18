/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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