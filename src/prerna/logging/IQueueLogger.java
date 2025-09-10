package prerna.logging;

import java.util.Properties;

public interface IQueueLogger {
	
	public void init();
	public void send(String key,String value);
	public void close();
	
	 public default Properties getProperties(String config){
	        Properties prop = new Properties();
	        for(String entry: config.split(",")){
	            String[] keyValue = entry.split("=");
	            prop.put(keyValue[0].trim(), keyValue[1].trim());
	        }
	        return prop;
	    }

}
