package prerna.reactor.frame.gaas.processors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Collectors;

import prerna.util.Utility;

public class TextFileProcessor {
	
	String fileName = null;
	CSVWriter writer = null;
	
	public TextFileProcessor(String fileName, CSVWriter writer)
	{
		this.fileName = fileName;
		this.writer = writer;
	}
	
	public void process() throws IOException {
		String source = getSource(fileName);
		
		String fileContent = null;
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
        	fileContent = reader.lines().collect(Collectors.joining("\n"));
        }

        // for a text document there is only ever one page / divider
        String pageIndex = "1";
        writer.writeRow(source, pageIndex, fileContent, "");
	}	
	
	private String getSource(String fileName)
	{
		String source = null;
		File file = new File(fileName);
		if(file.exists())
			source = file.getName();
				
		source = Utility.cleanString(source, true);
	
		return source;
	}
}
