package prerna.configure;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Hashtable;
import java.util.Properties;

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
	
	public static void main(String [] args) throws Exception
	{
		// get in the args what the base folder is
		String homePath = null;
		
		if(args.length == 0) {
			System.out.println("Usage: java prerna.configure.Me <semoss home dir>");
			System.exit(0);
		}
		if(args != null && args.length > 0)
			homePath = args[0].replace("\\", "/");
		
		System.out.println("Using home folder: " + homePath);
		
//		if(homePath == null)
//			homePath = "C:/Users/pkapaleeswaran/workspacej3/MonolithDev2";

		Me cm = new Me();
		
		
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
		
		cm.changeJS(homePath, port);
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
		
		System.out.println("------------------------");
		System.out.println("SEMOSS configured! Run startSEMOSS.bat and point your browser to http://localhost:" + port + "/SemossWeb/ to access SEMOSS!");
		System.out.println("------------------------");
	}
	
	public void changeTomcatXML(String homePath, String port) throws Exception
	{
		// args[0]/conf/server
		String appFile = homePath + "/../conf/server.xml";
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
		boolean found = true;
		int port = 5355;int count = 0;
		String server = "10.13.229.203";
		server = "127.0.0.1";
		for(;found && count < 5;port++, count++)
		{
			System.out.print("Trying.. " + port);
			try
			{
				ServerSocket s = new ServerSocket(port) ;//"10.13.229.203", port);
				//s.connect(new InetSocketAddress(server, port), 5000);//"localhost", port);
				//s.accept();
				found = false;
				s.close();
				System.out.println("  Success !!!!");
			}catch (Exception ex)
			{
				// do nothing
				//ex.printStackTrace();
				System.out.println("  Fail");
				//System.exit(0);
				found = true;
				//ex.printStackTrace();
			}finally
			{
			}
		}
		port--;
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String portStr = null;
		if(count >= 5) {
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
		
		return portStr;
	}
	
	public void changeRDFMap(String homePath, String port)
	{
		System.out.println("Configuring RDF Map.. ");
		String rdfHome = homePath + "/RDF_Map.prop";
//		rdfHome = homePath + "/RDF_Map2.prop";
		
		String [] stringToReplace = {"BaseFolder", 
									"LOG4J", 
									"SMSSWebWatcher_DIR",
									"SMSSWatcher_DIR", 
									"CSVInsightsWebWatcher_DIR", 
									"INSIGHT_CACHE_DIR",
									"SOLR_BASE_URL"};
		String [] stringToReplaceWith = {homePath, 
										 homePath + "/log4j.prop", 
										 homePath + "/db", 
										 homePath + "/db",
										 homePath + "/InsightCache/CSV_Insights",
										 homePath + "/InsightCache",
										 "http://localhost:" + port + "/solr"}; 
		
		replaceProp(rdfHome, stringToReplace, stringToReplaceWith);
		
	}
	
	public void changeJS(String homePath, String port) throws Exception
	{
		// args[0]/webapps/semossweb/app/app.config.js
		// args[0]/webapps/semossweb/olddev/app/scripts/config.js
		// args[0]/conf/server
		//cm.configurePort(args[0]);
		
		System.out.println("Modifying Web Configuration.....");
		
		String appPath = homePath + "/../webapps/SemossWeb/app/app.config.js";
		String altPath = appPath + "temp";
		

//		String appPath = homePath + "/Webcontent/dev/app/app.config1.js";
//		String altPath = homePath + "/Webcontent/dev/app/app.config2.js";
		System.out.println("Web Config " + appPath);
		System.out.println("Web Config 2 " + altPath);
		
		String inout = null;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(appPath)));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(altPath)));
		
		inout = br.readLine();
		
		while(inout != null)
		{
			//System.out.println(inout);
			if(inout.contains(".constant('PORT'"))
				inout = ".constant('PORT','" + port + "')";
			bw.write(inout+"\n");
			
			inout = br.readLine();
		}
		
		bw.close();
		br.close();
		
		replaceFiles(appPath, altPath);
		
		
		// the old app.config
		appPath = homePath + "/../webapps/SemossWeb/olddev/app/scripts/config.js";
		altPath = appPath + "temp";
//		appPath = homePath + "/Webcontent/dev/olddev/app/scripts/config.js";
//		altPath = homePath + "/Webcontent/dev/olddev/app/scripts/config2.js";

		inout = null;
		
		br = new BufferedReader(new InputStreamReader(new FileInputStream(appPath)));
		bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(altPath)));
		
		inout = br.readLine();
		
		while(inout != null)
		{
			if(inout.contains(".constant('PORT'"))
				inout = ".constant('PORT','" + port + "')";
			bw.write(inout+"\n");
			
			inout = br.readLine();
		}
		
		bw.close();
		br.close();

		replaceFiles(appPath, altPath);		
	}
	
	public void replaceFiles(String fileToReplace, String fileToReplaceWith) throws Exception
	{
		// delete file to replace and replace with file to be replaced
		File toRep = new File(fileToReplace);
		File tobeRep = new File(fileToReplaceWith);
		Files.move(tobeRep.toPath(), toRep.toPath(), StandardCopyOption.REPLACE_EXISTING);
	}
	
	private void replaceProp(String fileName, String [] thingsToReplace, String []thingsToReplaceWith)
	{
		try {
			File readFile = new File(fileName);
			FileInputStream fis = new FileInputStream(readFile);
			Properties prop = new Properties();
			prop.load(fis);
			for(int repIndex = 0;repIndex < thingsToReplace.length;repIndex++)
				prop.put(thingsToReplace[repIndex], thingsToReplaceWith[repIndex]);
			
			// close the stream
			fis.close();
			
			FileOutputStream fos = new FileOutputStream(readFile);
			// write it back
			prop.store(fos, "Complete");
			fos.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
		String appFile = homePath + "/../webapps/Monolith/WEB-INF/web.xml";
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
		String browserBat = homePath + "/../openBrowser.bat";
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
