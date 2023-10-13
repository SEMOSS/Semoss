package prerna.reactor.frame.gaas.processors;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class ProjectDriver {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String file = "c:/temp/ABACUS_ADP_FY24.docx";
		Map map = addFileProperties(file);
		Gson gson = new Gson();
		Map map2 = new HashMap();
		map2.put("hello", "world");
		System.err.println("got the gson");
		System.err.println(gson.toJson(map2));
		System.err.println(gson.toJson(map));
		/*
		CSVWriter writer = new CSVWriter("c:/temp/abacus.csv");
		//PPTProcessor ppt = new PPTProcessor("C:\\Users\\pkapaleeswaran\\Desktop\\From C Drive Root\\prerna\\semoss\\Nov 2019\\Generative AI 101.pptx", writer);
		//PPTProcessor ppt = new PPTProcessor("C:\\temp\\GPS Gen AI MCO Brief May 2023.pptx", writer);
		//ExCo PreRead - Future of Offering Sprint1
		//PPTProcessor ppt = new PPTProcessor("C:\\temp\\ExCo PreRead - Future of Offering Sprint1.pptx", writer);
		//ppt.process();
		//PDFProcessor pdf = new PDFProcessor("c:\\temp\\Chat GPT Prompts.pdf", writer);
		//PDFProcessor pdf = new PDFProcessor("c:\\temp\\FAR.pdf", writer);
		//pdf.process();
		DocProcessor dp = new DocProcessor("c:/temp/ABACUS_ADP_FY24.docx", writer);
		dp.process();
		
		
		// things to write into config.json
		Map finalMap = new HashMap();	
		// file name
		// created time
		// last modified
		// size of the file - in case there is issue on it
		// MD5 : optional for now
		// 
		*/
		
	}
	
	public static Map addFileProperties(String fileLocation)
	{
		Map <String, Object> retMap = new HashMap<String, Object>();
		Path path = Paths.get(fileLocation);
		try
		{
			BasicFileAttributes attr = Files.readAttributes(path, BasicFileAttributes.class);
			retMap.put("create_time", attr.creationTime());
			retMap.put("last_modified_time", attr.lastModifiedTime());
			retMap.put("last_access_time", attr.lastAccessTime());
			retMap.put("size", attr.size());
			String mimeType = Files.probeContentType(path);
			retMap.put("mime_type", mimeType);
			retMap.put("name", path.getFileName() +"");
			retMap.put("location", path + ""); // need to remove the reference to project
		}catch(Exception ex)
		{
			
		}
		System.err.println(retMap);
		return retMap;
		
	}
	
}
