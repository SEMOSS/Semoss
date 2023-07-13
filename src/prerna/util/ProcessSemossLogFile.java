package prerna.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ProcessSemossLogFile {

	public static void main(String[] args) throws IOException {
		String directory = "C:\\Users\\mahkhalil\\Downloads\\logs";
		File mainDir = new File(directory);

		ProcessSemossLogFile processor = new ProcessSemossLogFile();
		Map<String, List<String>> found = new TreeMap<>();
		processor.processDirectory(mainDir, "Running >>> InventoryImport", found);
		
	    BufferedWriter writer = new BufferedWriter(new FileWriter("C:\\Users\\mahkhalil\\Downloads\\found.txt"));
	    for(String filename : found.keySet()) {
	    	writer.write(filename);
	    	writer.write("\n");
	    	List<String> foundLines = found.get(filename);
	    	for(String line : foundLines) {
	    		writer.write(line);
	    		writer.write("\n");
	    	}
	    	writer.flush();
	    }
	    writer.close();
	}

	public void processDirectory(File directory, String textToFind, Map<String, List<String>> found) throws IOException {
		File[] files = directory.listFiles();
		for(File f : files) {
			if(f.isDirectory()) {
				processDirectory(f, textToFind, found);
			} else {
				processFile(f, textToFind, found);
			}
		}

	}

	private void processFile(File f, String textToFind, Map<String, List<String>> found) throws IOException {
		String filePath = f.getAbsolutePath();
		if(filePath.endsWith(".log")) {
			System.out.println("Process ::: " + filePath);
			readFiles(f, textToFind, found);
		} else {
			System.out.println("Ignore ::: " + filePath);
		}
	}

	private void readFiles(File f, String textToFind, Map<String, List<String>> found) throws IOException {
		List<String> foundValues = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader(f))) {
			String line;
			while ((line = br.readLine()) != null) {
				// process the line
				if(line.contains(textToFind)) {
					System.out.println(line);
					foundValues.add(line);
				}
			}
		}
		found.put(f.getName(), foundValues);
	}

}
