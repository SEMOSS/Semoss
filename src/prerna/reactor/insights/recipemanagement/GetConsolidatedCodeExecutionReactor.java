package prerna.reactor.insights.recipemanagement;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.cluster.util.ClusterUtil;
import prerna.om.Pixel;
import prerna.om.PixelList;
import prerna.om.Variable;
import prerna.om.Variable.LANGUAGE;
import prerna.project.api.IProject;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class GetConsolidatedCodeExecutionReactor extends AbstractReactor {

	private static final Logger logger = LogManager.getLogger(GetConsolidatedCodeExecutionReactor.class);
	public static final String OUTPUT_FILES = "write";
	
	public GetConsolidatedCodeExecutionReactor() {
		this.keysToGet = new String[] {OUTPUT_FILES};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		boolean write = true;
		if(this.keyValue.containsKey(OUTPUT_FILES)) {
			write = Boolean.parseBoolean(this.keyValue.get(OUTPUT_FILES));
		}
		List<StringBuffer> consolidatedCode = new ArrayList<>();
		
		PixelList pList = this.insight.getPixelList();
		int size = pList.size();
		if(size == 0) {
			return new NounMetadata(consolidatedCode, PixelDataType.VECTOR);
		}
		
		int counter = 0;
		final String CODE_SUFFIX = "_cc";
		String codeDir = null;
		File curFile = null;
		FileWriter fw = null;
		BufferedWriter bw = null;
		StringBuffer buffer = null;
		
		if(write) {
			codeDir = AssetUtility.getAssetBasePath(this.insight, null, true) + "/codeConsolidation";
			if(this.insight.isSavedInsight()) {
				IProject project = Utility.getProject(this.insight.getProjectId());
				ClusterUtil.pullProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId()));
			}
			
			File codeD = new File(codeDir);
			if(codeD.exists() && codeD.isDirectory()) {
				// should we delete everything here?
				File[] existingFiles = codeD.listFiles();
				for(File f : existingFiles) {
					if(f.getName().endsWith(CODE_SUFFIX)) {
						f.delete();
					}
				}
			} else {
				codeD.mkdirs();
			}
		}
		
		// keep combining until we get to a point where we switch languages
		try {
			LANGUAGE prevLanguage = null;
			for(int i = 0; i < size; i++) {
				Pixel p = pList.get(i);
				if(p.isCodeExecution()) {
					// combine into the same buffer
					if(prevLanguage == p.getLanguage()) {
						String pCode = p.getCodeExecuted();
						buffer.append(pCode).append("\n");
						bw.write(pCode);
					} else {
						buffer = new StringBuffer();
						consolidatedCode.add(buffer);
						String pCode = p.getCodeExecuted();
						buffer.append(pCode).append("\n");
						prevLanguage = p.getLanguage();
						
						if(write) {
							if(bw != null) {
								try {
									bw.flush();
									bw.close();
								} catch (IOException e) {
									logger.error(Constants.STACKTRACE, e);
								}
							}
							if(fw != null) {
								try {
									fw.close();
								} catch (IOException e) {
									logger.error(Constants.STACKTRACE, e);
								}
							}
						}
						
						curFile = new File(codeDir + "/file" + (counter++) + CODE_SUFFIX + "." + Variable.getExtension(prevLanguage) );
						fw = new FileWriter(curFile);
						bw = new BufferedWriter(fw);
						bw.write(pCode);
					}
				}
			}
		} catch(IOException e) {
			logger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error occurred trying to write the insights code to a file");
		} finally {
			if(bw != null) {
				try {
					bw.flush();
					bw.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
			if(fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					logger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		if(write && this.insight.isSavedInsight()) {
			IProject project = Utility.getProject(this.insight.getProjectId());
			ClusterUtil.pushProjectFolder(project, AssetUtility.getProjectVersionFolder(project.getProjectName(), project.getProjectId()));
		}
		
		List<String> retCode = new ArrayList<>();
		for(int i = 0; i < consolidatedCode.size(); i++) {
			retCode.add(consolidatedCode.get(i).toString());
		}
		
		return new NounMetadata(retCode, PixelDataType.VECTOR);
	}
	
}
