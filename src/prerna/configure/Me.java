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

import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class Me {
	// the idea here really simple
	// look at where the batch file is 
	// and then based on that run configuration
	public static String os = "";
	
	public static void main(String [] args) throws Exception
	{
		Me cm = new Me();
		
		// Before anything else, see what the OS is
		cm.os = System.getProperty("os.name").toLowerCase();
		
		// get in the args what the base folder is
		String homePath = null;
		String rHome = null;
		String rLib = null;
		String jriDll = null;
		String rdll = null;
		
		if(args == null || args.length < 5) {
			System.out.println("Usage: java prerna.configure.Me <semoss home dir> <r home> <r library> <Location to R Library dll/so> <Location to JRI library dll/so>");
			System.exit(0);
		}
		else if(args != null && args.length == 5){
			//System.out.println("CAUTION!!! MULTIPLE ARGUEMENTS BEING PASSED, MIGHT BE ERROR.. WILL TRY TO RUN WITH FIRST ARGUMENT\n"
			//		+ "POSSIBLE ISSUE WITH SEMOSS HOME DIRECTORY HAVING SPACES...");
			System.out.println("First argument passed into method is: " + args[0]);
			homePath = args[0].replace("\\", "/");
			rHome = args[1].replace("\\", "/");
			rLib = args[2].replace("\\", "/");
			rdll = args[3].replace("\\", "/");
			jriDll = args[4].replace("\\", "/");
		}
		
		System.out.println("Using home folder: " + homePath);
		
//		if(homePath == null)
//			homePath = "C:/Users/pkapaleeswaran/workspacej3/MonolithDev2";
		
		
		// so this si where the batch file is running
		// I have a few things now i need to change
		String port = cm.findOpenPort();
		
		System.out.println("Found port.. " + port);
		// RDF Map is sitting in 
		// args[0] / semosshome	
		cm.changeRDFMap(homePath, port);
		
		// the web.xml is sitting in 
		// args[0]/webapps/monolith/webcontent/web-inf
		
		cm.changeWebXML(homePath);
		
//		cm.changeJS(homePath, port);
		// adjust the ports
		// this is to adjust app.config.js
		// this is sitting in
		// args[0]/webapps/semossweb/app/app.config.js
		// args[0]/webapps/semossweb/olddev/app/scripts/config.js
		// args[0]/conf/server
		//cm.configurePort(args[0]);
		
		// last thing is tomcat
		cm.changeTomcatXML(homePath,port);
		
		cm.genOpenBrowser(homePath, port);
		
		cm.writeConfigureFile(homePath, port);
		
		cm.writePath(homePath, rHome, rdll, jriDll, rLib);
		
		// write base env such as
		// path
		//  
		
		// need to write classpath and home files
		// need to change the loadpath on catalina to include the library path
		cm.writeTomcatEnv(homePath, rdll, jriDll);		
		
		System.out.println("------------------------");
		System.out.println("SEMOSS configured! Run startSEMOSS.bat and point your browser to http://localhost:" + port + "/SemossWeb/ to access SEMOSS!");
		System.out.println("------------------------");
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
			e.printStackTrace();
		} finally {
			try {
				if(bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void writeTomcatEnv(String semossHome, String rHome, String jriHome)
	{
		//-Djava.library.path=C:\Users\pkapaleeswaran\Documents\R\win-library\3.1\rJava\jri\x64;"C:\Program Files\R\R-3.2.4revised\bin\x64"
		String fileName = semossHome + "/../tomcat/bin/" + "setenv.bat";
		BufferedWriter bw = null;
		try {
			//BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new PrintStream(System.out)));
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName)));
			String prefix = "";
			if (System.getenv().containsKey("PYTHON_HOME")) {
				prefix += "\"%PYTHON_HOME%/\";\"%PYTHON_HOME%/Scripts/\";\"%PYTHON_HOME%/Lib/site-packages/jep\";";
			}
			String options = "-Djava.library.path=" + prefix + "\"" + rHome + "\";\"" + jriHome + "\"";
			// should we also set the memory here ?
			// to get max memory ?
			//options = options + " " + "-Xms256m -Xmx512m";
			bw.write("set JAVA_OPTS=" + options);
			bw.flush();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if(bw != null) {
					bw.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
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
			e.printStackTrace();
		} finally {
			try {
				if(writer != null) {
					writer.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void changeTomcatXML(String homePath, String port) throws Exception
	{
		// args[0]/conf/server
		String appFile = homePath + "/../tomcat/conf/server.xml";
		// changing for my current box
//		appFile = "C:/Users/pkapaleeswaran/Desktop/From C Drive Root/apache-tomcat-8.0.15/conf/server2.xml";
		System.out.println("Configuring Tomcat.. " + appFile);
		
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document d = db.parse(appFile);
		NodeList nl = d.getElementsByTagName("Connector");
		for(int nodeIndex = 0;nodeIndex < nl.getLength();nodeIndex++)
		{
			Node n = nl.item(nodeIndex);
			//System.out.println("Node is.. " + n.getTextContent());
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
		
		System.out.println("Finding an open port.. ");
		boolean found = false;
		int port = 5355;int count = 0;
		String server = "10.13.229.203";
		server = "127.0.0.1";
		for(;!found && count < 5;port++, count++)
		{
			System.out.print("Trying.. " + port);
			try
			{
				ServerSocket s = new ServerSocket(port) ;//"10.13.229.203", port);
				//s.connect(new InetSocketAddress(server, port), 5000);//"localhost", port);
				//s.accept();
				found = true;
				s.close();
				System.out.println("  Success !!!!");
				//no error, found an open port, we can stop
				break;
			}catch (Exception ex)
			{
				// do nothing
				//ex.printStackTrace();
				System.out.println("  Fail");
				//System.exit(0);
				found = false;
				//ex.printStackTrace();
			}finally
			{
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
				System.out.println("Unable to find an open port. Please provide a port.");
				 try {
					portStr = br.readLine();
				} catch (IOException e) {
					e.printStackTrace();
				}
				System.out.println("Using port: " + portStr);
			} else {
				portStr = port+"";
			}
		} finally {
			try {
				if(is != null) {
					is.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				if(br != null) {
					br.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return portStr;
	}
	
	public void changeRDFMap(String homePath, String port, String rdfHome) {
		System.out.println("Configuring RDF Map.. ");
		
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
		// args[0]/webapps/semossweb/app/app.config.js
		// args[0]/webapps/semossweb/olddev/app/scripts/config.js
		// args[0]/conf/server
		//cm.configurePort(args[0]);
		
		System.out.println("Modifying Web Configuration.....");
		
		String appPath = homePath + "/../tomcat/webapps/SemossWeb/core/app.config.js";
		String altPath = appPath + "temp";
		

//		String appPath = homePath + "/Webcontent/dev/app/app.config1.js";
//		String altPath = homePath + "/Webcontent/dev/app/app.config2.js";
		System.out.println("Web Config " + appPath);
		System.out.println("Web Config 2 " + altPath);
		
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
			e.printStackTrace();
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
				e.printStackTrace();
			}
		}
		replaceFiles(appPath, altPath);
		
		// the embed app.config
		appPath = homePath + "/../tomcat/webapps/SemossWeb/embed/app.config.js";
		altPath = appPath + "temp";
//		appPath = homePath + "/Webcontent/dev/olddev/app/scripts/config.js";
//		altPath = homePath + "/Webcontent/dev/olddev/app/scripts/config2.js";

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
			e.printStackTrace();
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
				e.printStackTrace();
			}
		}
		
		replaceFiles(appPath, altPath);
		
		// the playsheet app.config
		appPath = homePath + "/../tomcat/webapps/SemossWeb/playsheet/app.config.js";
		altPath = appPath + "temp";
		//				appPath = homePath + "/Webcontent/dev/olddev/app/scripts/config.js";
		//				altPath = homePath + "/Webcontent/dev/olddev/app/scripts/config2.js";

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
			e.printStackTrace();
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
				e.printStackTrace();
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
		List<String> content = new ArrayList<String>();
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
			e.printStackTrace();
		} finally{
			// close the readers
			try{
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}

			try{
				fileOut.close();
			} catch (IOException e){
				e.printStackTrace();
			}
		}
	}

	public void changeWebXML(String homePath) throws Exception
	{
		// the web.xml is sitting in 
		// args[0]/webapps/monolith/webcontent/web-inf
		
		
		Hashtable thingsToWatch= new Hashtable();
		thingsToWatch.put("file-upload", homePath + "/upload");
		thingsToWatch.put("temp-file-upload", homePath + "/upload");
		thingsToWatch.put("RDF-MAP", homePath + "/RDF_Map.prop");
		

		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		String appFile = homePath + "/../tomcat/webapps/Monolith/WEB-INF/web.xml";
		System.out.println("Configuring web.xml " + appFile);
		Document d = db.parse(appFile);
		NodeList nl = d.getElementsByTagName("context-param");
		for(int nodeIndex = 0;nodeIndex < nl.getLength();nodeIndex++)
		{
			Node n = nl.item(nodeIndex);
			//System.out.println("Node is.. " + n.getTextContent());

			NodeList cl = n.getChildNodes();
			//System.out.println("New Node.. >>>>>>>>>>>>>>>>>");
			
			for(int childIndex = 0;childIndex < cl.getLength();childIndex++)
			{
				Node c = cl.item(childIndex);
				//System.out.println("child is.. " + c.getNodeName() + ">>" + c.getTextContent());

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
		//FileWriter fw = new FileWriter();
		TransformerFactory tf = TransformerFactory.newInstance();
		DOMSource source = new DOMSource(d);
		StreamResult sr = new StreamResult(new File(altFile));
		
		
		
		tf.newTransformer().transform(source, sr);
		
		replaceFiles(appFile, altFile);
	}
	
	private void genOpenBrowser(String homePath, String port) throws Exception
	{
		String browserBat = homePath + "/config/openBrowser.bat";
		BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(browserBat)));
		
		writer.write("ECHO Opening browser to http://localhost:" + port + "/SemossWeb/ to access SEMOSS...");
		writer.write("\n\n");
		writer.write("ECHO OFF\n\n");
		writer.write("IF EXIST \"C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe\" (");
		writer.write("  START \"Chrome\" chrome --new-window \"http://localhost:" + port + "/SemossWeb/\"");	
		writer.write("			) ELSE (");
		writer.write("  START \"\" \"http://localhost:" + port + "/SemossWeb/\"");
		writer.write(")");
		writer.flush();writer.close();
		
	} 
}
