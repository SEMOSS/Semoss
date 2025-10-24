package prerna.engine.impl;

import prerna.engine.api.IEngine;
import prerna.engine.api.IMCP;
import prerna.project.api.IProject;

public class MCPFactory {

	private MCPFactory() {

	}

	public static IMCP build(IEngine engine) {
		if (engine instanceof IProject) {
			return (IMCP) engine;
		}

		if (!engine.isMCPEnabled()) {
			throw new IllegalArgumentException(
					"MCP access needs to be enabled for this resource before it can be used");
		}

		InternalMCP mcp = new InternalMCP(engine);
		return mcp;
	}

}
