package prerna.auth.utils;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.cluster.util.ClusterUtil;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.project.api.IProject;
import prerna.project.impl.Project;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.AssetUtility;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class WorkspaceAssetUtils extends AbstractSecurityUtils {
	
	private static final Logger classLogger = LogManager.getLogger(WorkspaceAssetUtils.class);

	public static final String WORKSPACE_APP_NAME = "Workspace";
	public static final String ASSET_APP_NAME = "Asset";
	public static final String HIDDEN_FILE = ".semoss";
	
	WorkspaceAssetUtils() {
		super();
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Creating workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	
	/**
	 * Create the user workspace project for the provided user and auth token
	 * @param user
	 * @param provider
	 * @return
	 * @throws Exception
	 */
	public static String createUserWorkspaceProject(User user, AuthProvider provider) throws Exception {
		String projectId = createEmptyProject(user, provider, WORKSPACE_APP_NAME, false);
		registerUserWorkspaceProject(user.getAccessToken(provider), projectId);
		return projectId;
	}
	
	/**
	 * Create the user asset project for the provided user and auth token
	 * @param user
	 * @param provider
	 * @return
	 * @throws Exception
	 */
	public static String createUserAssetProject(User user, AuthProvider provider) throws Exception {
		String projectId = createEmptyProject(user, provider, ASSET_APP_NAME, true);
		registerUserAssetProject(user.getAccessToken(provider), projectId);
		return projectId;
	}
	
	/**
	 * Generate empty project that is for asset/workspace
	 * @param user
	 * @param provider
	 * @param projectName
	 * @param isAsset
	 * @return
	 * @throws Exception
	 */
	private static String createEmptyProject(User user, AuthProvider provider, String projectName, boolean isAsset) throws Exception {
		AccessToken token = user.getAccessToken(provider);
		// Create a new project id
		String projectId = UUID.randomUUID().toString();

		String userFolderLocation = AssetUtility.getUserAssetAndWorkspaceBaseFolder(projectName, projectId);
		File userFolder = new File(userFolderLocation);
		userFolder.mkdirs();

		// Add database into DIHelper so that the web watcher doesn't try to load as well
		File tempSmss = SmssUtilities.createTemporaryAssetAndWorkspaceSmss(projectId, projectName, isAsset, null);
		DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, tempSmss.getAbsolutePath());
		
		// Add the project to security db
		if(!isAsset) {
			SecurityProjectUtils.addProject(projectId, false, user);
			SecurityProjectUtils.addProjectOwner(projectId, token.getId());
		}
		
		// Create the project
		Project project = new Project();
		
		// Only at end do we add to DIHelper
		DIHelper.getInstance().setProjectProperty(projectId, project);
		String projects = (String) DIHelper.getInstance().getProjectProperty(Constants.PROJECTS);
		projects = projects + ";" + projectId;
		DIHelper.getInstance().setProjectProperty(Constants.PROJECTS, projects);
		
		// Rename .temp to .smss
		File smssFile = new File(tempSmss.getAbsolutePath().replace(".temp", ".smss"));
		FileUtils.copyFile(tempSmss, smssFile);
		tempSmss.delete();
		
		// Update engine smss file location
		project.open(smssFile.getAbsolutePath());
			
		if (ClusterUtil.IS_CLUSTER) {
			ClusterUtil.pushUserWorkspace(projectId, isAsset);
		}
		
		DIHelper.getInstance().setProjectProperty(projectId + "_" + Constants.STORE, smssFile.getAbsolutePath());
		return projectId;
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Updating workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	// TODO >>>timb: WORKSPACE - DONE - register workspace

	/**
	 * Register the user workspace project for the provided access token and project id
	 * @param token
	 * @param projectId
	 * @throws SQLException 
	 */
	public static void registerUserWorkspaceProject(AccessToken token, String projectId) throws SQLException {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO WORKSPACEENGINE(TYPE, USERID, PROJECTID) VALUES(?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, token.getProvider().name());
			ps.setString(parameterIndex++, token.getId());
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Register the user workspace project for the provided user, auth provider, and project id
	 * @param user
	 * @param provider
	 * @param projectId
	 * @throws SQLException 
	 */
	public static void registerUserWorkspaceProject(User user, AuthProvider provider, String projectId) throws SQLException {
		registerUserWorkspaceProject(user.getAccessToken(provider), projectId);
	}
	
	/**
	 * Register the user asset project for the provided access token and project id
	 * @param token
	 * @param projectId
	 * @throws SQLException 
	 */
	public static void registerUserAssetProject(AccessToken token, String projectId) throws SQLException {
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("INSERT INTO ASSETENGINE(TYPE, USERID, PROJECTID) VALUES(?,?,?)");
			int parameterIndex = 1;
			ps.setString(parameterIndex++, token.getProvider().name());
			ps.setString(parameterIndex++, token.getId());
			ps.setString(parameterIndex++, projectId);
			ps.execute();
			if(!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
		} catch (SQLException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}
	
	/**
	 * Register the user asset project for the provided user, auth provider, and project id
	 * @param user
	 * @param provider
	 * @param projectId
	 * @throws SQLException 
	 */
	public static void registerUserAssetProject(User user, AuthProvider provider, String projectId) throws SQLException {
		registerUserAssetProject(user.getAccessToken(provider), projectId);
	}

	
	//////////////////////////////////////////////////////////////////////
	// Querying workspace and asset metadata 
	//////////////////////////////////////////////////////////////////////
	
	/**
	 * Get the user workspace project for the provided access token; returns null if there is none
	 * @param token
	 * @return
	 */
	public static String getUserWorkspaceProject(AccessToken token) {
//		String query = "SELECT PROJECTID FROM WORKSPACEENGINE WHERE "
//				+ "TYPE = '" + token.getProvider().name() + "' AND "
//				+ "USERID = '" + token.getId() + "'"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACEENGINE__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACEENGINE__TYPE", "==", token.getProvider().name()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACEENGINE__USERID", "==", token.getId()));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				 Object rs = wrapper.next().getValues()[0];
				 if (rs == null){
					 return null;
				 }
				return rs.toString();
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
		
		return null;
	}
	
	/**
	 * Get the user workspace project for the provided user and auth provider; returns null if is there is none
	 * @param user
	 * @param provider
	 * @return
	 */
	public static String getUserWorkspaceProject(User user, AuthProvider provider) {
		return getUserWorkspaceProject(user.getAccessToken(provider));
	}
	
	/**
	 * Get the user asset project for the provided access token; returns null if there is none
	 * @param user
	 * @param token
	 * @return
	 */
	public static String getUserAssetProject(AccessToken token) {
//		String query = "SELECT PROJECTID FROM ASSETENGINE WHERE "
//				+ "TYPE = '" + token.getProvider().name() + "' AND "
//				+ "USERID = '" + token.getId() + "'"
//				;
//		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ASSETENGINE__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ASSETENGINE__TYPE", "==", token.getProvider().name()));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ASSETENGINE__USERID", "==", token.getId()));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				 Object rs = wrapper.next().getValues()[0];
				 if (rs == null){
					 return null;
				 }
				return rs.toString();
				//return wrapper.next().getValues()[0].toString();
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
		
		return null;
	}
	
	/**
	 * Get the user asset project for the provided user and auth provider; returns null if there is none
	 * @param user
	 * @param provider
	 * @return
	 */
	public static String getUserAssetProject(User user, AuthProvider provider) {
		return getUserAssetProject(user.getAccessToken(provider));
	}
	
	/**
	 * See if the project is a workspace or asset
	 * @param projectId
	 * @return
	 */
	public static boolean isAssetOrWorkspaceProject(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("WORKSPACEENGINE__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("WORKSPACEENGINE__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return true;
			} else {
				return isAssetProject(projectId);
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
		
		return false;
	}
	
	/**
	 * Is the project an asset
	 * @param projectId
	 * @return
	 */
	public static boolean isAssetProject(String projectId) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("ASSETENGINE__PROJECTID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("ASSETENGINE__PROJECTID", "==", projectId));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			return wrapper.hasNext();
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

		return false;
	}
	
	
	//////////////////////////////////////////////////////////////////////
	// Asset folder locations
	//////////////////////////////////////////////////////////////////////
	public static String getUserAssetRootDirectory(User user, AuthProvider provider) {
		String assetProjectId = user.getAssetProjectId(provider);
		if (assetProjectId != null) {
			IProject assetProject = Utility.getProject(assetProjectId);
			if (assetProject != null) {
				String assetProjectName = assetProject.getProjectName();
				if (assetProjectName != null) {
					return AssetUtility.getProjectVersionFolder(assetProjectName, assetProjectId);
				}
			}
		}
		return null;
	}

}
