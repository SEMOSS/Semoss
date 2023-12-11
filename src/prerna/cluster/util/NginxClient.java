package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;
import prerna.util.Utility;

public class NginxClient implements Watcher{
	
	/*
	// Environment Variables this depnds on
	 * zk - semicolon separated list of zk to use for registration
	 * home - what is the main registration root - default is assumed as /semoss_root
	 * app - The root for various apps - default is assumed as /app
	 * host -  ip and port - this is useful if you are running multiple containers on the same box
	
	// registers to semoss_root/semoss- as a ephemeral_sequential
	// the semoss_root is an environment variable - this will allow me to spin as many cluster as I want
	 * Major version
	 * minor version
	 * semoss_sequential node
	 * host ip and port
	 * number of cpus
	 * memory
	 * User who is booted on it
	 * The registration is typically 
	 * register_root / #cpu / #memory / semoss-node
	 * register_root / semoss-node
	 * nginx will only watch the resigter_root/semoss_node initially
	 *  I could even do this for specific databases so the same thing is not loaded multiple times
	// zk e
	
	*/

	public static final String ZK_SERVER = "zk";
	public static final String HOST = "host";
	public static final String TIMEOUT = "to";
	public static final String BOOTUSER = "bu";
	public static final String HOME = "home";
	public static final String APP_HOME = "app";

	
	ZooKeeper zk = null;
	Map <String, String> env = null;
	public String zkServer = "192.168.99.100:2181";
	public String host = "192.168.99.100:8888";
	public String user = "generic";
	public String home = "/semoss_root";
	public String app = "/app";
	//public String semossHome = "/opt/semosshome/";
	public String semossHome = "c:/users/pkapaleeswaran/workspacej3/docker/";
	
	
	boolean connected = false;
	
	public static NginxClient zkClient = null;
	
	
	
	int version = 0;

	protected NginxClient()
	{
		
	}
	
	public static NginxClient getInstance()
	{
		if(zkClient == null)
		{
			zkClient = new NginxClient();
			zkClient.init();
			if(zkClient.connected)
			{
				zkClient.watchForChildren();
				return zkClient;
			}
		}
		return null;
	}
	
//	public static void main(String [] args) throws Exception
//	{
//		NginxClient c = NginxClient.getInstance();
//		System.out.println("Wait here.. ");
//		c.waitHere();
//	}
	
	public void waitHere()
	{
		try {
			
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			br.readLine();
			
		}catch(Exception ignored)
		{
			
		}
	}
	
	public void init()
	{
		// initiates connection to zk and makes the connection
		try {
			env = System.getenv();
			if(env.containsKey(ZK_SERVER))
				zkServer = env.get(ZK_SERVER);
			
			if(env.containsKey(HOST))
				host = env.get(HOST);
			
			int timeout = (30 * 60 * 1000);

			if(env.containsKey(TIMEOUT))
				host = env.get(TIMEOUT);

			if(env.containsKey(BOOTUSER))
				user = env.get(BOOTUSER);

			if(env.containsKey(HOME))
				home = env.get(HOME);

			if(env.containsKey(APP_HOME))
				app = env.get(APP_HOME);

			if(zkServer != null && host != null)
			{
				// open zk
				// default time is 30 min
				zk = new ZooKeeper(zkServer, timeout, this);
				
				connected = true;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void publishNode()
	{
		// right now I dont have everything.. 
		// but this publishes, major, minor, ip:port, cpu, memory
		
		try {
			zk.create(home +"/semoss" , getPayload().getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// also need to publish to the user
		// usally this is home/user
	}
	
	// I need another where I say publish database
	public void publishDB(String engineID)
	{
		// so assume I tell this as /app/<dbname> - does that do the trick ?
		// so when I want to load a github I say /user@github.com/app/<dbname>
		try {
			zk.create(home + app + "/" + engineID ,  host.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getPayload()
	{
		// this will make all the major minor versions etc. 
		StringBuilder sb = new StringBuilder();
		sb.append("cpu=").append(Runtime.getRuntime().availableProcessors()).append("|");
		sb.append("memory=").append(Runtime.getRuntime().maxMemory()).append("|");
		sb.append("rver=").append("3.5").append("|");
		sb.append("semoss=").append("3.5").append("|"); // woo hoo.. same version as R
		sb.append("url=").append(host).append("|");
		sb.append("user").append(user).append("|");
		
		return sb.toString();
	}
	
	
	// watch the home and list things
	public void watchForChildren()
	{
		// watch the home
		// every event.. get the list of children compose the conf file
		// watch again
		// the event goes to processEvent
		try {
			zk.getChildren(home + app , true, null);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void openZK()
	{
		try {
			zk = new ZooKeeper("192.168.99.100:6311", 6311, this);
			
			//Thread.sleep(60000);
			getVersion("/pk");
			List <ACL> aclList = ZooDefs.Ids.OPEN_ACL_UNSAFE;
			//zk.create("/pk", "helo".getBytes(), aclList, CreateMode.EPHEMERAL);
			Stat stat = new Stat();
			stat.setVersion(3);
			byte [] b = zk.getData("/pk", true, stat);
			String data = new String(b, "UTF-8");
			System.out.println(" >>" + data);
			Thread.sleep(2000);
			
			//zk.setData("/pk", "world".getBytes(), version);
			
			List <String> childs  = zk.getChildren("/pk", false);
			
			for(int childIndex = 0;childIndex < childs.size();childIndex++)
				System.out.println("Child >> " + childs.get(childIndex));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void getVersion(String path)
	{
		try {
			version = zk.exists(path,true).getVersion();
			System.out.println("Running current version at " + version);
		} catch (KeeperException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	// bind events tos epcific things
	

	@Override
	public void process(WatchedEvent event) {	
		// TODO Auto-generated method stub
		// System.out.println("Ok.. something came back on this" + arg0);
		EventType type = event.getType();
		
		System.out.println("Some Event came in.. " + event.getType());
		
		if(type == EventType.NodeCreated)
			nodeChildChanged(Utility.getClassName(event.getPath()));
		
		else if(type == EventType.NodeDeleted)
			nodeChildChanged(Utility.getClassName(event.getPath()));

		else if(type == EventType.NodeChildrenChanged)
			nodeChildChanged(event.getPath());

		watchForChildren();
		
	}
	
	public void nodeCreated(String path)
	{
		System.out.println("Node Created..  " + path);
	}
	
	public void nodeDeleted(String path)
	{
		System.out.println("Node Deleted..  " + path);
	}
	
	public void nodeChildChanged(String path)
	{
		regenConfig(path);
	}
	
	public String getNodeData(String path)
	{
		String data = null;
		
		try {
			byte [] b = zk.getData(path, true, new Stat());
			data = new String(b, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return data;
	}
	
	public void regenConfig(String path)
	{
		Map <String, String> nameURL = new HashMap<String, String>();
		try {
			List <String> children = zk.getChildren(path, null);
			
			
			// now for each children
			// get the data and pull it from there
			for(int childIndex = 0;childIndex < children.size();childIndex++)
			{
				String childName = children.get(childIndex);
				System.out.println("Child is.. " + childName);
				String output = getNodeData(home + app + "/" + childName);
				System.out.println("And the URL I need to register is.. " + output);
				nameURL.put(childName, output);
			}
			
			genNginx(nameURL);
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public void genNginx(Map map)
	{
		//FileTemplateLoader ftl1 = new FileTemplateLoader(new File("/tmp/templates"));
        try {
			Configuration cfg = new Configuration();

			cfg.setIncompatibleImprovements(new Version(2, 3, 20));
			cfg.setDefaultEncoding("UTF-8");
			cfg.setLocale(Locale.US);
			cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
			cfg.setDirectoryForTemplateLoading(new File(semossHome + "nginx/templates"));

			Map <String, Object> input = new HashMap<String, Object>();
			

			Template t = cfg.getTemplate("upstream.conf");

			input.put("apps", map);
			backup();
			Writer out = new FileWriter(semossHome + "nginx/conf/nginx.conf");
			t.process(input, out);
			
			out.flush();
			out.close();
			//reloadNginx();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TemplateException e) {
			// TODO Auto-generated catch block
			
			e.printStackTrace();
		}
	}
	
	public void backup()
	{
		try
		{
			String curConfig = semossHome + "nginx/conf/nginx.conf";
			String backConfig = semossHome + "nginx/conf/nginx-working.conf";
			
			if(Files.exists(Paths.get(backConfig)))
				Files.delete(Paths.get(backConfig));
			
			Files.copy(Paths.get(curConfig), Paths.get(backConfig));
			
			
		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void reloadNginx()
	{
		// need to get the id - use the pidof
		// https://stackoverflow.com/questions/16965089/getting-pid-of-process-in-shell-script
		/*
		try {
			// and then execute a kill -HUP
			ProcessBuilder pb = new ProcessBuilder("pidof 'nginx: master process nginx' > " + semossHome + "nginxid");
			pb.start();
			
			BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(semossHome + "nginxid")));
			
			String nginxId = br.readLine();
			
			pb = new ProcessBuilder("kill -HUP " + nginxId);
			pb.start();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
	}

}
