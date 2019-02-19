package prerna.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class FindAllRPackages {

	private static final String LIBRARY_REGEX = "library\\((.*?)\\)";

	public static void main(String[] args) throws IOException {
		find("src", ".java");
		find("R", ".R");
	}
	
	private static void find(String folder, String extension) throws IOException {
		Pattern libraryPattern = Pattern.compile(LIBRARY_REGEX);
		
		Path root = Paths.get("");
		Path src = root.resolve(folder);
		
		Set<String> packages = new HashSet<>();
		Files.walk(src)
        .filter(f -> f.toFile().getName().endsWith(extension))
        .forEach(f -> {
        	try (Stream<String> stream = Files.lines(f, Charset.forName("Cp1252"))) {
    			stream.forEach(l -> {
    				Matcher libraryMatcher = libraryPattern.matcher(l);
    				while(libraryMatcher.find()) {
    					String packageName = libraryMatcher.group(1);
    		            System.out.println(f.getFileName().toString() + ", found raw: " + packageName);
    		            packages.add(packageName);
    		        }
    			});
    		} catch (IOException e) {
    			e.printStackTrace();
    		}
        });
		System.out.println("----------");
		packages.stream().forEach(System.out::println);		
	}
}