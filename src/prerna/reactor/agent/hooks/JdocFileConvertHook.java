package prerna.reactor.agent.hooks;

import java.io.File;
import java.util.Map;
import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.engine.api.IEngine;
import prerna.engine.api.IEngine.CATALOG_TYPE;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IAgentRunHook;
import prerna.util.EngineUtility;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;

import org.jodconverter.core.office.OfficeException;
import org.jodconverter.core.office.OfficeManager;
import org.jodconverter.local.LocalConverter;
import org.jodconverter.local.office.LocalOfficeManager;
import org.jodconverter.local.office.LocalOfficeUtils;


public final class JdocFileConvertHook implements IAgentRunHook {
	
	private static final Logger classLogger = LogManager.getLogger(GitCommitAgentHook.class);
	
    @Override
    public void afterRun(AgentRunContext ctx, AgentHarnessResult result) throws OfficeException {
    	classLogger.info("DISPATCHING JDOC FILE CONVERTER HOOK!!!");
    	Map<String, Object> paramMap = ctx.getAgentConfig().getModelParams();
    	String filePath = Objects.toString(paramMap.get("filePath"), null);
    	
    	if (filePath == null) {
    		classLogger.error("File path is missing from paramMap");
    		return;
    	}
    	
    	OfficeManager officeManager = LocalOfficeManager.builder()
    	        .portNumbers(2002, 2003)   
    	        .install()                
    	        .build();
    	officeManager.start();
    	

    	Room room = ctx.getRoom();
    	String roomPath = room.getRoomFolderPath();
    	String fullPath = roomPath + filePath;
    	
    	classLogger.info("FOUND FULL PATH!!");
    	
    	LocalConverter.make().convert(new File(fullPath)).to(new File(roomPath + "PDF_CONVERSION.pdf")).execute();
    }

}
