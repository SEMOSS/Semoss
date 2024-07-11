package prerna.reactor.frame.gaas.processors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.util.Utility;

public class TextFileProcessor {
	
	private static final Logger classLogger = LogManager.getLogger(PPTProcessor.class);

	private String filePath = null;
	private CSVWriter writer = null;
	
	public TextFileProcessor(String filePath, CSVWriter writer) {
		this.filePath = filePath;
		this.writer = writer;
	}
	
	public void process() throws IOException {
		String source = getSource(this.filePath);
		
		String fileContent = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(this.filePath))) {
        	fileContent = reader.lines().collect(Collectors.joining("\n"));
        }

        // for a text document there is only ever one page / divider
        String pageIndex = "1";
        this.writer.writeRow(source, pageIndex, fileContent, "");
	}
	
	/**
	 * 
	 * @param filePath
	 * @return
	 */
	private String getSource(String filePath) {
		String source = null;
		File file = new File(filePath);
		if(file.exists()) {
			source = file.getName();
		}
//		source = Utility.cleanString(source, true);
		return source;
	}
	
}
