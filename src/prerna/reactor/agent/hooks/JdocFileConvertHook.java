package prerna.reactor.agent.hooks;

import java.io.File;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import org.jodconverter.core.office.OfficeException;
//import org.jodconverter.core.office.OfficeManager;
//import org.jodconverter.core.office.OfficeUtils;
//import org.jodconverter.local.LocalConverter;
//import org.jodconverter.local.office.LocalOfficeManager;

import prerna.cluster.util.ClusterUtil;
import prerna.engine.impl.model.Room;
import prerna.reactor.agent.AgentHarnessResult;
import prerna.reactor.agent.AgentRunContext;
import prerna.reactor.agent.IAgentRunHook;
import prerna.util.unoserver.Unoserver;


public final class JdocFileConvertHook implements IAgentRunHook {

	private static final Logger classLogger = LogManager.getLogger(JdocFileConvertHook.class);
	
	@Override
	public void afterRun(AgentRunContext ctx, AgentHarnessResult result) {
		Map<String, Object> agentParams = ctx.getAgentConfig().getAgentParams();
		String filePath = Objects.toString(agentParams.get("filePath"), null);

		if (filePath == null) {
			classLogger.error("File path is missing from paramMap");
			return;
		}

		Room room = ctx.getRoom();
		String roomPath = room.getRoomFolderPath();
		
		Unoserver uno = new Unoserver();
		
		File source = new File(roomPath, filePath);
		String pdfName = FilenameUtils.getBaseName(filePath) + ".pdf";
		File target = new File(source.getParentFile(), pdfName);

		if (!source.exists()) {
			classLogger.error("Source file does not exist, skipping conversion: " + source.getAbsolutePath());
			return;
		}
		
		try {
			File pdf = uno.convertToFile(source, "pdf", target.getAbsolutePath());
			classLogger.info("Converted " + source.getAbsolutePath() + " -> " + pdf.getAbsolutePath());
			ClusterUtil.pushRoomAsync(room.getId());
		} catch (RuntimeException e) {
			classLogger.error("Failed to convert " + source.getAbsolutePath() + " to PDF via unoserver", e);
		}
	}

//	@Override
//	public void afterRun(AgentRunContext ctx, AgentHarnessResult result) throws OfficeException {
//		classLogger.info("DISPATCHING JDOC FILE CONVERTER HOOK!!!");
//		Map<String, Object> agentParams = ctx.getAgentConfig().getAgentParams();
//		String filePath = Objects.toString(agentParams.get("filePath"), null);
//
//		if (filePath == null) {
//			classLogger.error("File path is missing from paramMap");
//			return;
//		}
//
//		Room room = ctx.getRoom();
//		String roomPath = room.getRoomFolderPath();
//
//		File source = new File(roomPath, filePath);
//		// Keep the original file name, just swap the extension for .pdf
//		// (e.g. "myReport.docx" -> "myReport.pdf"), written next to the source.
//		String pdfName = FilenameUtils.getBaseName(filePath) + ".pdf";
//		File target = new File(source.getParentFile(), pdfName);
//
//		if (!source.exists()) {
//			classLogger.error("Source file does not exist, skipping conversion: " + source.getAbsolutePath());
//			return;
//		}
//
//		classLogger.info("Converting " + source.getAbsolutePath() + " -> " + target.getAbsolutePath());
//
//		OfficeManager officeManager = LocalOfficeManager.builder()
//				.portNumbers(2002)
//				.build();
//		try {
//			officeManager.start();
//			LocalConverter.builder()
//					.officeManager(officeManager)
//					.build()
//					.convert(source)
//					.to(target)
//					.execute();
//			classLogger.info("Conversion complete: " + target.getAbsolutePath());
//		} finally {
//			OfficeUtils.stopQuietly(officeManager);
//		}
//	}

}
