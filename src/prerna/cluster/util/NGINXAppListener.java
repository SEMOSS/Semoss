package prerna.cluster.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;
import prerna.util.Constants;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NGINXAppListener extends NGINXDomainListener implements IZKListener {
	protected static final Logger logger = LogManager.getLogger(NGINXAppListener.class);
	
	//static String semossHome = "/opt/semosshome/";
	static String semossHome = "c:/users/pkapaleeswaran/workspacej3/docker/";
	
	
	public static final String SEMOSS_HOME = "sem";
	
	
	@Override
	public void process(String path, ZooKeeper zk) {
		
		regenConfig(path, zk);
		
		
	}
	
	public void regenConfig(String path, ZooKeeper zk)
	{
		// when it comes here
		// I need to navigate up a level to get to the domains and regenerate
		
		
		Map <String, Map<String, String>> domain = new HashMap<String, Map<String, String>>();
		System.out.println("Pulling specific APP.." + path);

		// navigate one level up
		String watchPath = path;
		String [] pathTokens = path.split("/");
		String lastPath = pathTokens[pathTokens.length - 1];		
		path = path.replace("/" + lastPath, "");
		
		lastPath = pathTokens[pathTokens.length - 2];
		path = path.replace("/" + lastPath, "");
		
		System.out.println("Final Domain Pull.. " + path);
		
		try {
			
			// get all the domains first
			domains2Watch = zk.getChildren(path, null);
			for(int domainIndex = 0;domainIndex < domains2Watch.size();domainIndex++)
			{
				String childPath = domains2Watch.get(domainIndex);
			
				Map <String, String> nameURL = new HashMap<String, String>();
				List <String> children = zk.getChildren(path + "/" + childPath, null);
				// now for each children
				// get the data and pull it from there
				for(int childIndex = 0;childIndex < children.size();childIndex++)
				{
					String childName = children.get(childIndex);
					String newPath = path + "/" + childPath + "/" + childName;
					System.out.println("Child is.. " + childName);
					String output = getNodeData(newPath, zk);
					System.out.println("And the URL I need to register is.. " + output);
					nameURL.put(childName, output);
				}
				
				if(!childPath.equalsIgnoreCase("app") && nameURL.size() > 0)
					domain.put(childPath, nameURL);
				
			}	
			genNginx(domain);
			watchDomains(watchPath);
			
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		}
		
		
		
	}
	
	protected void watchDomains(String path)
	{
		System.out.println("Registering Domains.. ");
		//Thread.sleep(3000);
		// register all the domains again
		//ZKClient.getInstance().watchEvent(path, EventType.NodeChildrenChanged, this, false);
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
			

			Template t = cfg.getTemplate("upstream2.conf");

			input.put("apps", map);
			backup();
			Writer out = new FileWriter(semossHome + "nginx/conf/nginx.conf");
			t.process(input, out);
			
			out.flush();
			out.close();
			//reloadNginx();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		} catch (TemplateException e) {
			// TODO Auto-generated catch block
			
			logger.error(Constants.STACKTRACE, e);
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
			logger.error(Constants.STACKTRACE, ex);
		}
	}
	
	public void reloadNginx()
	{
		/*
		// need to get the id - use the pidof
		// https://stackoverflow.com/questions/16965089/getting-pid-of-process-in-shell-script
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
			logger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		}
		*/
	}

	public static String getNodeData(String path, ZooKeeper zk)
	{
		String data = null;
		
		try {
			byte [] b = zk.getData(path, true, new Stat());
			data = new String(b, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error(Constants.STACKTRACE, e);
		}
		
		return data;
	}
	



}
