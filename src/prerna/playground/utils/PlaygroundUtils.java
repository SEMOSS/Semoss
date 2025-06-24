package prerna.playground.utils;

import com.amazonaws.services.guardduty.model.Feedback;
import com.google.gson.Gson;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.Utility;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO: Remove SelectQueryStruct

public class PlaygroundUtils {

    private static Logger classLogger = LogManager.getLogger(PlaygroundUtils.class);

  public static String openRoom(String insightId, AccessToken userToken, RDBMSNativeEngine modelInferenceLogsDb, String projectId, String projectName) {

    String query = "INSERT INTO ROOM (INSIGHT_ID, "
				+ "USER_ID, USER_NAME, USER_EMAIL_ID, "
				+ "AGENT_TYPE, AGENT_ID, IS_ACTIVE, "
				+ "DATE_CREATED, PROJECT_ID, PROJECT_NAME) "
				+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
		// boolean allowClob = modelInferenceLogsDb.getQueryUtil().allowClobJavaObject();
		PreparedStatement ps = null;
		try {
			ps = modelInferenceLogsDb.getPreparedStatement(query);
			int index = 1;
			ps.setString(index++, insightId);
            ps.setString(index++, userToken.getId());
			ps.setString(index++, userToken.getName());
            if (userToken.getEmail() != null) {
                ps.setString(index++, userToken.getEmail());
            } else {
                ps.setNull(index++, java.sql.Types.VARCHAR);                
            }
			ps.setString(index++, modelInferenceLogsDb.getCatalogSubType(modelInferenceLogsDb.getSmssProp()));
	        ps.setString(index++, modelInferenceLogsDb.getEngineId());
			ps.setBoolean(index++, true);
			ps.setTimestamp(index++, Utility.getCurrentSqlTimestampUTC());
			ps.setString(index++, projectId);
			ps.setString(index++, projectName);
			ps.execute();
			if (!ps.getConnection().getAutoCommit()) {
				ps.getConnection().commit();
			}
        return insightId;
	} catch (Exception e) {
        classLogger.error(Constants.STACKTRACE, e);
    }
    return insightId;
  }

  public static boolean deactivateRoom(String roomId, String userId, RDBMSNativeEngine modelInferenceLogsDb)
      throws SQLException {

    String ps =
        "UPDATE ROOM SET IS_ACTIVE = ? WHERE INSIGHT_ID = ? AND USER_ID = ?";

    try (PreparedStatement updatePs = modelInferenceLogsDb.getPreparedStatement(ps)) {
      updatePs.setBoolean(1, false);
      updatePs.setString(2, roomId);
      updatePs.setString(3, userId);
      updatePs.executeUpdate();
    }
    return true;
  }
}
