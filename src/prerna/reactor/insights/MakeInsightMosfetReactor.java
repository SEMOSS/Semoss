package prerna.reactor.insights;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.MosfetFile;
import prerna.project.api.IProject;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.git.GitRepoUtils;
import prerna.util.git.GitUtils;

public class MakeInsightMosfetReactor extends AbstractInsightReactor {

	private static final Logger classLogger = LogManager.getLogger(MakeInsightMosfetReactor.class);

	public MakeInsightMosfetReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.PROJECT.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.OVERRIDE.getKey()};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String projectId = getProject();
		String rdbmsId = null;
		try {
			rdbmsId = getRdbmsId();
		} catch(Exception e) {
			// ignore
		}
		boolean override = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]));
		
		int numUpdated = 0;
		int numIgnored = 0;
		int numError = 0;
		
		if(rdbmsId == null) {
			// we will run for all the insights
			// so you need to be the app owner
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
			
			if(!SecurityProjectUtils.userIsOwner(this.insight.getUser(), projectId)) {
				throw new IllegalArgumentException("User must be an owner of the app to update all the app mosfet files");
			}
			
			IProject project = Utility.getProject(projectId);
			String projectName = project.getProjectName();
			List<String> insightIds = project.getInsights();
			
			for(String id : insightIds) {
				
				String mosfetPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
						+ DIR_SEPARATOR + Constants.PROJECT_FOLDER
						+ DIR_SEPARATOR + SmssUtilities.getUniqueName(projectName, projectId)
						+ DIR_SEPARATOR + "version" 
						+ DIR_SEPARATOR + Utility.normalizePath(id);
				
				if(!override) {
					File f = new File(mosfetPath + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);
					if(f.exists()) {
						numIgnored++;
						continue;
					}
				}
				
				MosfetFile mosfet = null;
				try {
					mosfet = generateMosfetFromInsight(project, id);
				} catch(IllegalArgumentException e) {
					numIgnored++;
				}
				
				if (mosfet == null) {
					throw new NullPointerException("mosfet file should not be null here");
				}
				
				try {
					mosfet.write(mosfetPath, true);
					numUpdated++;
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
					numError++;
				}
				
				// add to git
				String gitFolder = AssetUtility.getProjectVersionFolder(projectName, projectId);
				List<String> files = new Vector<>();
				files.add(rdbmsId + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);		
				GitRepoUtils.addSpecificFiles(gitFolder, files);
				GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Writing new " + mosfet.getInsightName() + " mosfet file"));
			}
		} else {
			// need edit access to the insight
			// override is assumed to be true
			if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
				throwAnonymousUserError();
			}
			
			if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), projectId, rdbmsId)) {
				throw new IllegalArgumentException("User does not have permission to edit this insight");
			}
			
			IProject project = Utility.getProject(projectId);
			MosfetFile mosfet = null;
			try {
				mosfet = generateMosfetFromInsight(project, rdbmsId);
			} catch(IllegalArgumentException e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw e;
			}
			
			String mosfetPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + Constants.PROJECT_FOLDER 
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(project.getProjectName(), projectId)
					+ DIR_SEPARATOR + "version" 
					+ DIR_SEPARATOR + rdbmsId;
			
			try {
				mosfet.write(mosfetPath, true);
				numUpdated++;
			} catch (IOException e) {
				classLogger.error(Constants.STACKTRACE, e);
				numError++;
			}
			
			// add to git
			String gitFolder = AssetUtility.getProjectVersionFolder(project.getProjectName(), projectId);
			List<String> files = new Vector<>();
			files.add(rdbmsId + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);		
			GitRepoUtils.addSpecificFiles(gitFolder, files);
			GitRepoUtils.commitAddedFiles(gitFolder, GitUtils.getDateMessage("Writing new " + mosfet.getInsightName() + " mosfet file"));
		}
		
		NounMetadata noun = new NounMetadata(true, PixelDataType.BOOLEAN);
		if(numError > 0) {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully updated " + numUpdated 
					+ " and ignored " + numIgnored + " mosfet files. Failed on " + numError + " mosfet files."));
		} else {
			noun.addAdditionalReturn(NounMetadata.getSuccessNounMessage("Successfully updated " + numUpdated 
				+ " and ignored " + numIgnored + " mosfet files"));
		}
		return noun;
	}

	/**
	 * Generate a mosfet file from an insight
	 * @param app
	 * @param insightId
	 * @return
	 */
	private MosfetFile generateMosfetFromInsight(IProject project, String insightId) {
		String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT, CACHEABLE, QUESTION_PKQL FROM QUESTION_ID WHERE ID IN ('" + insightId+ "');";
		MosfetFile mosfet = new MosfetFile();

		RDBMSNativeEngine insightRdbms = project.getInsightDatabase();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(insightRdbms, query);
			while(wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				
				mosfet.setProjectId(project.getProjectId());
				mosfet.setRdbmsId(values[0].toString());
				mosfet.setInsightName(values[1].toString());
				mosfet.setLayout(values[2].toString());
				if(insightRdbms.getQueryUtil().allowArrayDatatype()) {
					Object[] pixel = (Object[]) values[4];
					mosfet.setRecipe(Arrays.stream(pixel).map(o -> o + "").collect(Collectors.toList()));
				} else {
					Clob pixelArray = (Clob) values[4];
					InputStream pixelArrayIs = null;
					if(pixelArray != null) {
						try {
							pixelArrayIs = pixelArray.getAsciiStream();
						} catch (SQLException e) {
							classLogger.error(Constants.STACKTRACE, e);
						}
					}
					// flush input stream to string
					Gson gson = new Gson();
					InputStreamReader reader = new InputStreamReader(pixelArrayIs);
					List<String> pixel = gson.fromJson(reader, List.class);
					mosfet.setRecipe(pixel);
				}
			}
		} catch (Exception e1) {
			classLogger.error(Constants.STACKTRACE, e1);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		if(mosfet.getRecipe() == null || mosfet.getRecipe().size() == 0) {
			throw new IllegalArgumentException("Cannot create a mosfet for an empty insight");
		}
		
		// now add the metadata
		String description = null;
		List<String> tags = new Vector<>();
		
		query = "SELECT DISTINCT INSIGHTID, METAKEY, METAVALUE, METAORDER FROM INSIGHTMETA WHERE INSIGHTID='" + insightId + "' ORDER BY METAORDER";
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(insightRdbms, query);
			while(wrapper.hasNext()) {
				Object[] values = wrapper.next().getValues();
				String value = (String) values[2];
				
				if(values[1].toString().equals("tag")) {
					tags.add(value);
				} else if(values[1].toString().equals("description")) {
					description = value;
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		if(description != null) {
			mosfet.setDescription(description);
		}
		if(!tags.isEmpty()) {
			mosfet.setTags(tags.toArray(new String[]{}));
		}
		
		return mosfet;
	}
	
}
