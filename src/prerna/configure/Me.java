package prerna.configure;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Me {
	// the idea here really simple
	// look at where the batch file is 
	// and then based on that run configuration
	public static String os = "";
	private static Logger logger = Logger.getLogger(Me.class);

	private static final String STACKTRACE = "StackTrace: ";

	public static void main(String [] args) throws Exception
	{
		Me cm = new Me();

		// Before anything else, see what the OS is
		Me.os = System.getProperty("os.name").toLowerCase();

		// get in the args what the base folder is
		String homePath = null;
		String rHome = null;
		String rLib = null;
		String jriDll = null;
		String rdll = null;
		
		if(args == null || args.length < 5) {
			logger.info("Usage: java prerna.configure.Me <semoss home dir> <r home> <r library> <Location to R Library dll/so> <Location to JRI library dll/so>");
			System.exit(0);
		}
		else if(args.length == 5){
			logger.info("First argument passed into method is: " + args[0]);
			homePath = args[0].replace("\\", "/");
			rHome = args[1].replace("\\", "/");
			rLib = args[2].replace("\\", "/");
			rdll = args[3].replace("\\", "/");
			jriDll = args[4].replace("\\", "/");
		}
		
		logger.info("Using home folder: " + homePath);
	
		// so this si where the batch file is running
		// I have a few things now i need to change
		String port = cm.findOpenPort();
		
		logger.info("Found port.. " + port);
		// RDF Map is sitting in 
		// args[0] / semosshome	
		cm.changeRDFMap(homePath, port);

		// the web.xml is sitting in 
		// args[0]/webapps/monolith/webcontent/web-inf

		cm.changeWebXML(homePath);

		// adjust the ports
		// this is to adjust app.config.js
		// this is sitting in
		// args[0]/webapps/semossweb/app/app.config.js
		// args[0]/webapps/semossweb/olddev/app/scripts/config.js
		// args[0]/conf/server

		// last thing is tomcat
		cm.changeTomcatXML(homePath,port);
		
		cm.genOpenBrowser(homePath, port);
		
		cm.writeConfigureFile(homePath, port);
		
		cm.writePath(homePath, rHome, rdll, jriDll, rLib);
		
		// write base env such as
		// path
		// need to write classpath and home files
		// need to change the loadpath on catalina to include the library path
		cm.writeTomcatEnv(homePath, rdll, jriDll);		
		
		logger.info("------------------------");
		logger.info("SEMOSS configured! Run startSEMOSS.bat and point your browser to http://localhost:" + port + "/SemossWeb/ to access SEMOSS!");
		logger.info("------------------------");
	}
	
	public void writePath(String semossHome, String rHome, String rDll, String jriDll, String rLib)
	{
		String fileName = semossHome + "/setPath.bat";
		BufferedWriter bw = null;
		try{
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
			String path = "\nset path=%path%"; 
			path = path + ";" + semossHome;
			path = path + ";" + rHome;
			path = path + ";" + rDll;
			path = path + ";" + jriDll;
			
			String rHomePath = "\nset R_HOME=" + rHome;
			String rlibPath = "\nset R_LIBS=" + rLib;
			bw.write("\necho Modifying classpath");
			bw.write(path);
			bw.write(rHomePath);
			bw.write("\necho R_HOME IS %R_HOME%");
			bw.write(rlibPath);
			bw.write("\necho R_LIBS IS %R_LIBS%");
			bw.flush();
		} catch(IOException e) {
			logger.error(STACKTRACE, e);
		} finally {
			try {
				if(bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}
	}
	
	public void writeTomcatEnv(String semossHome, String rHome, String jriHome)
	{
		//-Djava.library.path=C:\Users\pkapaleeswaran\Documents\R\win-library\3.1\rJava\jri\x64;"C:\Program Files\R\R-3.2.4revised\bin\x64"
		String fileName = semossHome + "/../tomcat/bin/" + "setenv.bat";
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
			String prefix = "";
			if (System.getenv().containsKey("PYTHONHOME")) {
				prefix += "\"%PYTHONHOME%/\";\"%PYTHONHOME%/Scripts/\";\"%PYTHONHOME%/Lib/site-packages/jep\";";
			}
			else if (System.getenv().containsKey("PYTHON_HOME")) {
				prefix += "\"%PYTHON_HOME%/\";\"%PYTHON_HOME%/Scripts/\";\"%PYTHON_HOME%/Lib/site-packages/jep\";";
			}
			String options = "-Djava.library.path=" + prefix + "\"" + rHome + "\";\"" + jriHome + "\"";
			// should we also set the memory here? to get max memory? options = options + " " + "-Xms256m -Xmx512m";
			bw.write("set JAVA_OPTS=" + options);
			bw.flush();
		} catch (IOException e) {
			logger.error(STACKTRACE, e);
		} finally {
			try {
				if(bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}
		
	}
	
	public void writeConfigureFile(String homePath, String port) {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(homePath + "/configured.txt"));
			writer.write("Port = " + port);
			writer.write("SEMOSS Web = " + "http://localhost:" + port + "/SemossWeb/");
			writer.flush();
			writer.close();
		} catch (IOException e) {
			logger.error(STACKTRACE, e);
		} finally {
			try {
				if(writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}
	}
	
	public void changeTomcatXML(String homePath, String port) throws Exception
	{
		// args[0]/conf/server
		String appFile = homePath + "/../tomcat/conf/server.xml";
		// changing for my current box
		logger.info("Configuring Tomcat.. " + appFile);
		
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document d = db.parse(appFile);
		NodeList nl = d.getElementsByTagName("Connector");
		for(int nodeIndex = 0;nodeIndex < nl.getLength();nodeIndex++)
		{
			Node n = nl.item(nodeIndex);
			NamedNodeMap nnm = n.getAttributes();
			Node portNode = nnm.getNamedItem("port");
			portNode.setNodeValue(port);
		}
		
		// write it back
		String altFile = appFile + "temp";
		TransformerFactory tf = TransformerFactory.newInstance();
		DOMSource source = new DOMSource(d);
		StreamResult sr = new StreamResult(new File(altFile));
		
		tf.newTransformer().transform(source, sr);
		
		replaceFiles(appFile, altFile);
	}
	
	public String findOpenPort()
	{
		// start with 7677 and see if you can find any
		logger.info("Finding an open port.. ");
		boolean found = false;
		int port = 5355;
		int count = 0;
		for(;!found && count < 5;port++, count++)
		{
			logger.info("Trying.. " + port);
			try {
				ServerSocket s = new ServerSocket(port);
				found = true;
				s.close();
				logger.info("  Success !!!!");
				//no error, found an open port, we can stop
				break;
			} catch (Exception ex) {
				logger.error(STACKTRACE, ex);
				found = false;
			}
		}
		
		//if we found a port, return that port
		if(found) return port+"";
				
		port--;
		InputStreamReader is = null;
		BufferedReader br = null;
		String portStr = null;
		try {
			is = new InputStreamReader(System.in);
			br = new BufferedReader(is);
			if(!found) {
				logger.info("Unable to find an open port. Please provide a port.");
				 try {
					portStr = br.readLine();
				} catch (IOException e) {
					logger.error(STACKTRACE, e);
				}
				logger.info("Using port: " + portStr);
			} else {
				portStr = port+"";
			}
		} finally {
			try {
				if(is != null) {
					is.close();
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
			try {
				if(br != null) {
					br.close();
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}
		return portStr;
	}
	
	public void changeRDFMap(String homePath, String port, String rdfHome) {
		logger.info("Configuring RDF Map.. ");
		
		String [] stringToReplace = {"BaseFolder", 
									"LOG4J", 
									"SMSSWebWatcher_DIR",
									"SMSSWatcher_DIR", 
									"CSVInsightsWebWatcher_DIR", 
									"INSIGHT_CACHE_DIR",
									"SOLR_BASE_URL",
									"ADDITIONAL_REACTORS",
									"SOCIAL",
									"JobSchedulerWatcher_DIR",
									"rpa.config.directory"};
		String [] stringToReplaceWith = {homePath, 
										 homePath + "/log4j.prop", 
										 homePath + "/db", 
										 homePath + "/db",
										 homePath + "/InsightCache/CSV_Insights",
										 homePath + "/InsightCache",
										 "http://localhost:" + port + "/solr",
										 homePath + "/reactors.json",
										 homePath + "/social.properties",
										 homePath + "/rpa/json",
										 homePath + "/rpa"}; 
		
		replaceProp(rdfHome, stringToReplace, stringToReplaceWith);
	}
	
	public void changeRDFMap(String homePath, String port)
	{
		changeRDFMap(homePath, port, homePath + "/RDF_Map.prop");
		
	}
	
	public void changeJS(String homePath, String port) throws Exception
	{		
		logger.info("Modifying Web Configuration.....");

		String appPath = homePath + "/../tomcat/webapps/SemossWeb/core/app.config.js";
		String altPath = appPath + "temp";
		logger.info("Web Config " + appPath);
		logger.info("Web Config 2 " + altPath);

		String input = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
		FileInputStream fis = null;
		BufferedWriter bw = null;
		OutputStreamWriter osw = null;
		FileOutputStream fos = null;

		try {
			fis = new FileInputStream(appPath);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			fos = new FileOutputStream(altPath);
			osw = new OutputStreamWriter(fos);
			bw = new BufferedWriter(osw);
			
			input = br.readLine();
			while(input != null)
			{
				if(input.contains(".constant('PORT'")) {
					input = ".constant('PORT','" + port + "')";
				}
				bw.write(input+"\n");
				input = br.readLine();
			}
			
			bw.flush();
			bw.close();
			br.close();
		} catch (FileNotFoundException e) {
			logger.error(STACKTRACE, e);
		} finally {
			try {
				if(fis != null) {
					fis.close();
				}
				if(isr != null) {
					isr.close();
				}
				if(br != null) {
					br.close();
				}
				if(fos != null) {
					fos.close();
				}
				if(osw != null) {
					osw.close();
				}
				if(bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}
		replaceFiles(appPath, altPath);
		
		// the embed app.config
		appPath = homePath + "/../tomcat/webapps/SemossWeb/embed/app.config.js";
		altPath = appPath + "temp";
		input = null;

		try {
			fis = new FileInputStream(appPath);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			fos = new FileOutputStream(altPath);
			osw = new OutputStreamWriter(fos);
			bw = new BufferedWriter(osw);
			
			input = br.readLine();
			while(input != null)
			{
				if(input.contains(".constant('PORT'")) {
					input = ".constant('PORT','" + port + "')";
				}
				bw.write(input+"\n");
				input = br.readLine();
			}
			
			bw.flush();
			bw.close();
			br.close();
		} catch (FileNotFoundException e) {
			logger.error(STACKTRACE, e);
		} finally {
			try {
				if(fis != null) {
					fis.close();
				}
				if(isr != null) {
					isr.close();
				}
				if(br != null) {
					br.close();
				}
				if(fos != null) {
					fos.close();
				}
				if(osw != null) {
					osw.close();
				}
				if(bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}
		
		replaceFiles(appPath, altPath);
		
		// the playsheet app.config
		appPath = homePath + "/../tomcat/webapps/SemossWeb/playsheet/app.config.js";
		altPath = appPath + "temp";
		input = null;

		try {
			fis = new FileInputStream(appPath);
			isr = new InputStreamReader(fis);
			br = new BufferedReader(isr);
			fos = new FileOutputStream(altPath);
			osw = new OutputStreamWriter(fos);
			bw = new BufferedWriter(osw);
			
			input = br.readLine();
			while(input != null)
			{
				if(input.contains(".constant('PORT'")) {
					input = ".constant('PORT','" + port + "')";
				}
				bw.write(input+"\n");
				input = br.readLine();
			}
			
			bw.flush();
			bw.close();
			br.close();
		} catch (FileNotFoundException e) {
			logger.error(STACKTRACE, e);
		} finally {
			try {
				if(fis != null) {
					fis.close();
				}
				if(isr != null) {
					isr.close();
				}
				if(br != null) {
					br.close();
				}
				if(fos != null) {
					fos.close();
				}
				if(osw != null) {
					osw.close();
				}
				if(bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				logger.error(STACKTRACE, e);
			}
		}
		
		replaceFiles(appPath, altPath);	
	}

	public void replaceFiles(String fileToReplace, String fileToReplaceWith) throws Exception
	{
		// delete file to replace and replace with file to be replaced
		File toRep = new File(fileToReplace);
		File tobeRep = new File(fileToReplaceWith);
		Files.move(tobeRep.toPath(), toRep.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}
	
	private void replaceProp(String fileName, String [] keysForThingsToReplace, String [] newValuesForReplacement)
	{
		FileOutputStream fileOut = null;
		File file = new File(fileName);

		/*
		 * 1) Loop through the smss file and add each line as a list of strings
		 * 2) For each line, see if it starts with the key to alter
		 * 3) if yes, write out the key with the new value passed in
		 * 4) if no, just write out the line as is
		 * 
		 */
		List<String> content = new ArrayList<>();
		BufferedReader reader = null;
		FileReader fr = null;
		try{
			fr = new FileReader(file);
			reader = new BufferedReader(fr);
			String line;
			// 1) add each line as a different string in list
			while((line = reader.readLine()) != null){
				content.add(line);
			}

			fileOut = new FileOutputStream(file);
			byte[] lineBreak = "\n".getBytes();
			// 2) iterate through each line if the smss file
			for(int i = 0; i <content.size(); i++){
				// 3) if this line starts with the key to alter
				int length = keysForThingsToReplace.length;
				
				boolean foundKeyToAlter = false;
				KEY_LOOP : for(int keyToAlterIndex = 0; keyToAlterIndex < length; keyToAlterIndex++) {
					String keyToAlter = keysForThingsToReplace[keyToAlterIndex];
					if(content.get(i).contains(keyToAlter)){
						// create new line to write using the key and the new value
						String newKeyValue = keyToAlter + "\t" + newValuesForReplacement[keyToAlterIndex];
						fileOut.write(newKeyValue.getBytes());
						foundKeyToAlter = true;
						break KEY_LOOP;
					}
				}
				
				// 4) if it doesn't, just write the next line as is
				if(!foundKeyToAlter) {
					byte[] contentInBytes = content.get(i).getBytes();
					fileOut.write(contentInBytes);
				}
				// after each line, write a line break into the file
				fileOut.write(lineBreak);
			}
		} catch(IOException e){
			logger.error(STACKTRACE, e);
		} finally{
			// close the readers
			if (reader != null) {
				try{
					reader.close();
				} catch (IOException e) {
					logger.error(STACKTRACE, e);
				}
			}

			if (fileOut != null) {
				try{
					fileOut.close();
				} catch (IOException e){
					logger.error(STACKTRACE, e);
				}
			}
		}
	}

	public void changeWebXML(String homePath) throws Exception
	{
		// the web.xml is sitting in 
		// args[0]/webapps/monolith/webcontent/web-inf

		Hashtable<String, String> thingsToWatch= new Hashtable<>();
		thingsToWatch.put("file-upload", homePath + "/upload");
		thingsToWatch.put("temp-file-upload", homePath + "/upload");
		thingsToWatch.put("RDF-MAP", homePath + "/RDF_Map.prop");

		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		String appFile = homePath + "/../tomcat/webapps/Monolith/WEB-INF/web.xml";
		logger.info("Configuring web.xml " + appFile);
		Document d = db.parse(appFile);
		NodeList nl = d.getElementsByTagName("context-param");
		for(int nodeIndex = 0;nodeIndex < nl.getLength();nodeIndex++)
		{
			Node n = nl.item(nodeIndex);
			NodeList cl = n.getChildNodes();

			for(int childIndex = 0;childIndex < cl.getLength();childIndex++)
			{
				Node c = cl.item(childIndex);

				// usually if something is armed
				// I need to get the value of the one after that
				if(thingsToWatch.containsKey(c.getTextContent()))
				{
					Node repNode = cl.item(childIndex+2);
					repNode.setTextContent(""+thingsToWatch.get(c.getTextContent()));
				}
			}
		}

		// write it back
		String altFile = appFile + "temp";
		TransformerFactory tf = TransformerFactory.newInstance();
		DOMSource source = new DOMSource(d);
		StreamResult sr = new StreamResult(new File(altFile));

		tf.newTransformer().transform(source, sr);
		replaceFiles(appFile, altFile);
	}
	
	private void genOpenBrowser(String homePath, String port) throws Exception
	{
		String browserBat = homePath + "/config/openBrowser.bat";

		try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(browserBat)))) {
			writer.write("ECHO Opening browser to http://localhost:" + port + "/SemossWeb/ to access SEMOSS...");
			writer.write("\n\n");
			writer.write("ECHO OFF\n\n");
			writer.write("IF EXIST \"C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe\" (");
			writer.write("  START \"Chrome\" chrome --new-window \"http://localhost:" + port + "/SemossWeb/\"");	
			writer.write("			) ELSE (");
			writer.write("  START \"\" \"http://localhost:" + port + "/SemossWeb/\"");
			writer.write(")");
			writer.flush();
			writer.close();
		}		
	} 
}
