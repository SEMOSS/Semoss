package prerna.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

import com.eclipsesource.v8.JavaCallback;
import com.eclipsesource.v8.NodeJS;
import com.eclipsesource.v8.V8;
import com.eclipsesource.v8.V8Array;
import com.eclipsesource.v8.V8Object;

public class NodeTest {
	
	
//
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		V8 runtime = V8.createV8Runtime();
//		  runtime.executeVoidScript(""
//		    + "var person = {};\n"
//		    + "var hockeyTeam = {name : 'WolfPack',"
//		    + "say : function hello(){ return 'ok this is a method'; }"
//		    + ""
//		    + ""
//		    + "};\n"
//		    + "person.first = 'Ian';\n"
//		    + "person['last'] = 'Bull';\n"
//		    + "person.hockeyTeam = hockeyTeam;\n"
//		    + "");
//		  // TODO: Access the person object
//		 // runtime.release();
//		  
//		  V8Object person = runtime.getObject("person");
//		  V8Object hockeyTeam = person.getObject("hockeyTeam");
//		  Object retString = hockeyTeam.executeFunction("say", null);
//		  
//		  System.out.println("String " + retString);
//		  System.out.println(hockeyTeam.getString("name"));
//		  person.release();
//		  hockeyTeam.release();
//		  runtime.release();
//		  
//		  
//		  // try  node
//		  
//		  String NODE_SCRIPT = "var http = require('http');\n"
//				  + ""
//				  + "var server = http.createServer(function (request, response) {\n"
//				  + " response.writeHead(200, {'Content-Type': 'text/plain'});\n"
//				  + " response.end(someJavaMethod());\n"
//				  + "});\n"
//				  + ""
//				  + "server.listen(8000);\n"
//				  + "console.log('Server running at http://127.0.0.1:8000/');";
//
//		  long startTime = System.nanoTime();
//		  final NodeJS nodeJS = NodeJS.createNodeJS();
//		    
//		  
//		  File dotScript = new File("c:\\nodejs\\socket\\wiki.js");
//		  long nodeTime = System.nanoTime();
//		  V8Object wiki = nodeJS.require(dotScript);
//		  Object obj = wiki.executeJSFunction("search", "adele");
//		  BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//		  String data = "";
//		  long curTime = System.nanoTime();
//		  
//		  System.out.println("Node Startup .. " + (nodeTime - startTime)/1000000 + "Millis");
//		  System.out.println("Dot Startup .. " + (curTime - nodeTime)/1000000 + "Millis");
//		  
//		  JavaCallback callback = new JavaCallback() {
//		      
//			    public Object invoke(V8Object receiver, V8Array parameters) {
//			    	//V8Object obj = parameters.getObject(0);
//			    	//obj.executeJSFunction("pumpit");
//			    	Object obj = receiver.get("moron");
//			    	System.out.println(" Length >> " + parameters.length());
//			    	
//			    	V8Object socket = parameters.getObject(0);
//			    	
//			    	// the socket will come in here... but I have no idea what to do with the socket after
//			    	// may be feed to another function and do something ?
//			    	// I need another node script 
//			    	
//			    	// that I can call I bet ?
//			    	// or I need to start a thread here
//			    	
//			    	System.out.println(parameters.get(0));
//			    	
//			    	System.out.println("Object is.. " + obj);
//			    	
//			    	/*try {
//			    	 * 
//						Thread.sleep(1000000000);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}*/
//			    	
//			    	
//			    	
//			    	System.out.println("Hello World !!");
//			    	
//					  File replScript = new File("c:\\nodejs\\socket\\replclient.js");
//					  File slaveScript = new File("C:\\Users\\pkapaleeswaran\\AppData\\Roaming\\npm\\node_modules\\threads\\lib\\worker.node\\slave.js");
//					  V8Object script = nodeJS.require(replScript);
//					  nodeJS.require(slaveScript);
//
//					  //new JSObject((V8Object) exports.get("http"))
//					  Object vs = script.get("moron");
//					  
//					  System.out.println("Moron is .. " + vs);
//					  
//					  //script.executeFunction("s", null);
//					  // I need to pass the socket here
//					  script.executeJSFunction("simple", socket );
//						
//			    	
//			      return "Hello, JavaWorld!";
//			    }
//			  };
//			    
//			  nodeJS.getRuntime().registerJavaMethod(callback, "someJavaMethod");
//			    nodeJS.handleMessage();
//
//			  try {
//				  System.out.println("Enter a term to search.. " );
//				  
//				  // loop is a big NO NO.. 
//				  // you loop the handle message is toast
//				while((data = br.readLine()) != null)
//				{
//					System.out.println("Searching.. " + data);
//				    nodeJS.handleMessage();
//					obj = wiki.executeJSFunction("search", data);
//					System.out.println("Object " + obj);
//				}
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//
//			  File nodeScript = new File("c:\\nodejs\\socket\\index.js");
//			  nodeJS.exec(nodeScript);
//			  //script.executeJSFunction("java");
//			  while(nodeJS.isRunning()) {
//				    nodeJS.handleMessage();
//				  }
//
//
//			  /*V8Function callback2 = new V8Function(nodeJS.getRuntime(), new JavaCallback() {
//				  @Override
//				  public Object invoke(V8Object receiver, V8Array parameters) {
//				  final V8Object image = parameters.getObject(1);
//				  return null;
//				  }
//				  });
//			  */
//			  nodeJS.release();
//		  
//		  
//		  
//	}

}
