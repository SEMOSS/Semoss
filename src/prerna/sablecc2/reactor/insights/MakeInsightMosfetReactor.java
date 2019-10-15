package prerna.sablecc2.reactor.insights;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import com.google.gson.Gson;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityAppUtils;
import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.SmssUtilities;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.om.MosfetFile;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;
import prerna.util.sql.AbstractSqlQueryUtil;

public class MakeInsightMosfetReactor extends AbstractInsightReactor {

	public MakeInsightMosfetReactor() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP.getKey(), ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.OVERRIDE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = getApp();
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
			if(AbstractSecurityUtils.securityEnabled()) {
				if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
					throwAnonymousUserError();
				}
				
				if(!SecurityAppUtils.userIsOwner(this.insight.getUser(), appId)) {
					throw new IllegalArgumentException("User must be an owner of the app to update all the app mosfet files");
				}
			}
			
			IEngine app = Utility.getEngine(appId);
			List<String> insightIds = app.getInsights();
			
			for(String id : insightIds) {
				
				String mosfetPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
						+ DIR_SEPARATOR + "db"
						+ DIR_SEPARATOR + SmssUtilities.getUniqueName(app.getEngineName(), appId)
						+ DIR_SEPARATOR + "version" 
						+ DIR_SEPARATOR + id;
				
				if(!override) {
					File f = new File(mosfetPath + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);
					if(f.exists()) {
						numIgnored++;
						continue;
					}
				}
				
				MosfetFile mosfet = null;
				try {
					mosfet = generateMosfetFromInsight(app, id);
				} catch(IllegalArgumentException e) {
					numIgnored++;
				}
				
				try {
					mosfet.write(mosfetPath, true);
					numUpdated++;
				} catch (IOException e) {
					e.printStackTrace();
					numError++;
				}
			}
		} else {
			// need edit access to the insight
			// override is assumed to be true
			if(AbstractSecurityUtils.securityEnabled()) {
				if(AbstractSecurityUtils.anonymousUsersEnabled() && this.insight.getUser().isAnonymous()) {
					throwAnonymousUserError();
				}
				
				if(!SecurityInsightUtils.userCanEditInsight(this.insight.getUser(), appId, rdbmsId)) {
					throw new IllegalArgumentException("User does not have permission to edit this insight");
				}
			}
			
			IEngine app = Utility.getEngine(appId);
			MosfetFile mosfet = null;
			try {
				mosfet = generateMosfetFromInsight(app, rdbmsId);
			} catch(IllegalArgumentException e) {
				throw e;
			}
			
			String mosfetPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
					+ DIR_SEPARATOR + "db"
					+ DIR_SEPARATOR + SmssUtilities.getUniqueName(app.getEngineName(), appId)
					+ DIR_SEPARATOR + "version" 
					+ DIR_SEPARATOR + rdbmsId;
			
			try {
				mosfet.write(mosfetPath, true);
				numUpdated++;
			} catch (IOException e) {
				e.printStackTrace();
				numError++;
			}
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
	private MosfetFile generateMosfetFromInsight(IEngine app, String insightId) {
		String query = "SELECT DISTINCT ID, QUESTION_NAME, QUESTION_LAYOUT, CACHEABLE, QUESTION_PKQL FROM QUESTION_ID WHERE ID IN ('" + insightId+ "');";
		MosfetFile mosfet = new MosfetFile();

		RDBMSNativeEngine insightRdbms = app.getInsightDatabase();
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(insightRdbms, query);
		while(wrapper.hasNext()) {
			Object[] values = wrapper.next().getValues();
			
			mosfet.setEngineId(app.getEngineId());
			mosfet.setRdbmsId(values[0].toString());
			mosfet.setInsightName(values[1].toString());
			mosfet.setLayout(values[2].toString());
			if(insightRdbms.getQueryUtil().allowArrayDatatype()) {
				Object[] pixel = (Object[]) values[4];
				mosfet.setRecipe(Arrays.stream(pixel).toArray(String[]::new));
			} else {
				Clob pixelArray = (Clob) values[4];
				InputStream pixelArrayIs = null;
				if(pixelArray != null) {
					try {
						pixelArrayIs = pixelArray.getAsciiStream();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}
				// flush input stream to string
				Gson gson = new Gson();
				InputStreamReader reader = new InputStreamReader(pixelArrayIs);
				String[] pixel = gson.fromJson(reader, String[].class);
				mosfet.setRecipe(pixel);
			}
		}
		
		if(mosfet.getRecipe() == null || mosfet.getRecipe().length == 0) {
			throw new IllegalArgumentException("Cannot create a mosfet for an empty insight");
		}
		
		// now add the metadata
		String description = null;
		List<String> tags = new Vector<String>();
		
		query = "SELECT DISTINCT INSIGHTID, METAKEY, METAVALUE, METAORDER FROM INSIGHTMETA WHERE INSIGHTID='" + insightId + "' ORDER BY METAORDER";
		wrapper = WrapperManager.getInstance().getRawWrapper(insightRdbms, query);
		while(wrapper.hasNext()) {
			Object[] values = wrapper.next().getValues();
			
			String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[2]);
			
			if(values[1].toString().equals("tag")) {
				tags.add(value);
			} else if(values[1].toString().equals("description")) {
				description = value;
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
