package prerna.reactor.frame.gaas.processors;

import java.io.File;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;

public interface IFileHandler {
	
  boolean supportsFile(File file);

  int handleProcessing(File file, VectorDatabaseCSVWriter writer) throws Exception;
  
  IFileProcessor getFileProcessor(File file, VectorDatabaseCSVWriter writer);
}