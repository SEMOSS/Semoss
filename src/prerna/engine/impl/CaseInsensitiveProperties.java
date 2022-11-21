package prerna.engine.impl;

import java.io.Serializable;
import java.util.Properties;

public class CaseInsensitiveProperties extends Properties implements Serializable {

	/*
	 * Just upper case all the keys
	 */
	
	public CaseInsensitiveProperties() {
	
	}
	
	public CaseInsensitiveProperties(Properties prop) {
		for(Object key : prop.keySet()) {
			put(key, prop.get(key));
		}
	}
	
	@Override
	public synchronized Object setProperty(String key, String value) {
		return super.setProperty(key.toUpperCase(), value);
	}
	
	@Override
	public synchronized Object put(Object key, Object value) {
		if(key instanceof String) {
			return super.put(key.toString().toUpperCase(), value);
		} 
		return super.put(key, value);
	}
	
	@Override
	public synchronized Object putIfAbsent(Object key, Object value) {
		if(key instanceof String) {
			return super.putIfAbsent(key.toString().toUpperCase(), value);
		} 
		return super.putIfAbsent(key, value);
	}
	
	@Override
	public String getProperty(String key) {
		return super.getProperty(key.toUpperCase());
	}
	
	@Override
	public String getProperty(String key, String defaultValue) {
		return super.getProperty(key.toUpperCase(), defaultValue);
	}
	
	@Override
	public synchronized Object get(Object key) {
		if(key instanceof String) {
			return super.get(key.toString().toUpperCase());
		}
		return super.get(key);
	}
	
	@Override
	public synchronized Object getOrDefault(Object key, Object defaultValue) {
		if(key instanceof String) {
			return super.getOrDefault(key.toString().toUpperCase(), defaultValue);
		}
		return super.getOrDefault(key, defaultValue);
	}
	
	@Override
	public synchronized boolean containsKey(Object key) {
		if(key instanceof String) {
			return super.containsKey(key.toString().toUpperCase());
		}
		return super.containsKey(key);
	}
	
}
