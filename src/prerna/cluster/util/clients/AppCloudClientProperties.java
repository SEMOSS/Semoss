package prerna.cluster.util.clients;

import java.util.Map;

import prerna.util.DIHelper;

public class AppCloudClientProperties {

	private Map<String, String> env = null;
	private DIHelper helper = null;
	
	public AppCloudClientProperties() {
		this.env = System.getenv();
		this.helper = DIHelper.getInstance();
	}
	
	/**
	 * This method is used to first try and pull the value
	 * from the env 
	 * if it is not found or is empty then try to pull from DIHelper
	 * else return null
	 * @param key
	 * @return
	 */
	public String get(String key) {
		String val = this.env.get(key);
		if(val != null && !(val=val.trim()).isEmpty()) {
			return val;
		}
		// give benefit of the doubt..
		val = this.env.get(key.toUpperCase());
		if(val != null && !(val=val.trim()).isEmpty()) {
			return val;
		}
		val = this.env.get(key.toLowerCase());
		if(val != null && !(val=val.trim()).isEmpty()) {
			return val;
		}
		
		val = this.helper.getProperty(key);
		if(val != null && !(val=val.trim()).isEmpty()) {
			return val;
		}
		// give benefit of the doubt..
		val = this.helper.getProperty(key.toUpperCase());
		if(val != null && !(val=val.trim()).isEmpty()) {
			return val;
		}
		val = this.helper.getProperty(key.toLowerCase());
		if(val != null && !(val=val.trim()).isEmpty()) {
			return val;
		}
		
		// no luck...
		return null;
	}
	
}
