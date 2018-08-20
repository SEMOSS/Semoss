package prerna.cluster.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import freemarker.template.Version;

public class NGINXAppListener implements IZKListener {

	//static String semossHome = "/opt/semosshome/";
	static String semossHome = "c:/users/pkapaleeswaran/workspacej3/docker/";
	
	
	public static final String SEMOSS_HOME = "sem";
	
	static
	{
		if(System.getenv().containsKey(SEMOSS_HOME))
			semossHome = System.getenv().get(SEMOSS_HOME);		
	}
	
	@Override
	public void process(String path, ZooKeeper zk) {
		
		regenConfig(path, zk);
		
		
	}
	
	public void regenConfig(String path, ZooKeeper zk)
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
				String output = getNodeData(path + "/" + childName, zk);
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
	}

	public static String getNodeData(String path, ZooKeeper zk)
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
	



}
