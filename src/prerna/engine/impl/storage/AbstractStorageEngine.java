package prerna.engine.impl.storage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import prerna.engine.api.IEngine;
import prerna.engine.api.IStorageEngine;
import prerna.engine.impl.AbstractEngine;
import prerna.reactor.IReactor;
import prerna.reactor.agent.mcp.MCPUtility;
import prerna.reactor.storage.DeleteFromStorageReactor;
import prerna.reactor.storage.ListStoragePathDetailsReactor;
import prerna.reactor.storage.ListStoragePathReactor;
import prerna.reactor.storage.PullFromStorageReactor;
import prerna.reactor.storage.PushToStorageReactor;
import prerna.reactor.storage.SyncLocalToStorageReactor;
import prerna.reactor.storage.SyncStorageToLocalReactor;
import prerna.util.Utility;

import org.json.JSONObject;

public abstract class AbstractStorageEngine extends AbstractEngine implements IStorageEngine {

	/**
	 * Define MCP tools
	 */
	private List<Class<? extends IReactor>> mcpToolsList = new ArrayList<>(Arrays.asList(
		ListStoragePathReactor.class,
		ListStoragePathDetailsReactor.class,
		PullFromStorageReactor.class,
		PushToStorageReactor.class,
		SyncStorageToLocalReactor.class,
		SyncLocalToStorageReactor.class,
		DeleteFromStorageReactor.class
	));

	private JSONObject mcpTools;

	/**
	 * Init the general storage values
	 * @param builder
	 * @throws Exception 
	 */
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
	}
	
	// Converts comma-separated local file/folder paths to List<Path>
	protected List<Path> parseLocalPaths(String commaSeparatedPaths) throws Exception {
	    List<Path> result = new ArrayList<>();
	    String[] parts = commaSeparatedPaths.split(",");

	    for (String part : parts) {
	        String trimmed = part.trim();
	        if (!trimmed.isEmpty()) {
	            result.add(Paths.get(trimmed));
	        }
	    }

	    return result;
	}

	// Converts comma-separated cloud storage object paths to normalized String list
	protected List<String> parseStorageObjectPaths(String commaSeparatedPaths) {
	    List<String> result = new ArrayList<>();
	    String[] parts = commaSeparatedPaths.split(",");

	    for (String part : parts) {
	        String trimmed = part.trim();
	        if (!trimmed.isEmpty()) {
	        	// Normalize the path using the utility method
	            String normalized = Utility.normalizePath(trimmed);
	         // Remove the leading slash if present
	            if (normalized.startsWith("/")) {
	                normalized = normalized.substring(1);
	            }
	            result.add(normalized);
	        }
	    }

	    return result;
	}
	
	@Override
	public IEngine.CATALOG_TYPE getCatalogType() {
		return IEngine.CATALOG_TYPE.STORAGE;
	}
	
	@Override
	public String getCatalogSubType(Properties smssProp) {
		return this.getStorageType().toString();
	}
	
	@Override
	public boolean holdsFileLocks() {
		return false;
	}

	@Override
	public JSONObject getEngineMCPTools() {
		return MCPUtility.makeMCPJsonFromReactorClass(this.getEngineId(), this.mcpToolsList);
	}	
}
