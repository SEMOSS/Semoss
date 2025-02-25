package prerna.reactor.frame.gaas.processors;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.impl.vector.VectorDatabaseCSVWriter;
import prerna.util.Constants;

public class TextFileProcessor extends AbstractFileProcessor {
	
	private static final Logger classLogger = LogManager.getLogger(PPTProcessor.class);

	public TextFileProcessor(String filePath, VectorDatabaseCSVWriter writer) {
		super(filePath, writer);
	}
	
	@Override
	public void process() throws IOException {
		String source = getSource(this.filePath);
		
		String fileContent = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(this.filePath))) {
        	fileContent = reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
        	classLogger.error(Constants.STACKTRACE, e);
			throw e;
		}

        // for a text document there is only ever one page / divider
        String pageIndex = "1";
        this.writer.writeRow(source, pageIndex, fileContent);
	}
	
}
