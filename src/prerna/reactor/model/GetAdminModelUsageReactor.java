package prerna.reactor.model;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;

import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.model.inferencetracking.ModelInferenceLogsUtils;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class GetAdminModelUsageReactor extends AbstractReactor {

    private static final String USERS_KEY = "users";

    public GetAdminModelUsageReactor() {
        this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey(), USERS_KEY,
                ReactorKeysEnum.START_DATE.getKey(), ReactorKeysEnum.END_DATE.getKey() };
        this.keyRequired = new int[] { 1, 1, 0, 0 }; // engine and users required, dates optional
    }

    @Override
    public NounMetadata execute() {
        organizeKeys();
        User user = this.insight.getUser();
        if (user == null) {
            throw new IllegalArgumentException("You are not properly logged in");
        }

        if (!SecurityAdminUtils.userIsAdmin(user)) {
            throwFunctionalityOnlyExposedForAdminsError();
        }

        // Get and validate model engine
        String modelId = getModelEngineId();
        if (modelId == null || modelId.trim().isEmpty()) {
            throw new IllegalArgumentException("Must input a model id");
        }

        if (SecurityEngineUtils.getEngineType(modelId) != IEngine.CATALOG_TYPE.MODEL) {
            throw new IllegalArgumentException("Input engine must be a model engine");
        }

        // Get and validate users list
        List<String> userIds = getListString(USERS_KEY);
        if (userIds == null || userIds.isEmpty()) {
            throw new IllegalArgumentException("Must input at least one user id");
        }

        // Get and validate date parameters
        String startDate = this.keyValue.get(ReactorKeysEnum.START_DATE.getKey());
        String endDate = this.keyValue.get(ReactorKeysEnum.END_DATE.getKey());
        validateDateParameters(startDate, endDate);

        // Format dates for query (convert from YYYY-MM-DD to full datetime format if
        // provided)
        String formattedStartDate = startDate != null ? startDate + " 00:00:00.000" : null;
        String formattedEndDate = endDate != null ? endDate + " 23:59:59.999" : null;

        List<Map<String, Object>> usageList = ModelInferenceLogsUtils.getModelInferenceUserTokenReport(modelId,
                userIds, formattedStartDate, formattedEndDate);
        return new NounMetadata(usageList, PixelDataType.FORMATTED_DATA_SET);
    }

    @Override
    public String getReactorDescription() {
        return "Admin-only report of model tokens used by a specified list of users over a specified time period. "
                + "Requires model engine ID and list of user IDs. Date range is optional (if provided, both start and end dates must be given).";
    }

    @Override
    protected String getDescriptionForKey(String key) {
        if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
            return "Required model engine ID or alias";
        } else if (key.equals(USERS_KEY)) {
            return "Required list of user IDs to include in the report";
        } else if (key.equals(ReactorKeysEnum.START_DATE.getKey())) {
            return "Optional start date (format: YYYY-MM-DD). Must be provided with endDate.";
        } else if (key.equals(ReactorKeysEnum.END_DATE.getKey())) {
            return "Optional end date (format: YYYY-MM-DD). Must be provided with startDate.";
        }
        return super.getDescriptionForKey(key);
    }

    /**
     * Validates that if one date is provided, both must be provided,
     * that dates are valid, and that start date is before or equal to end date
     * 
     * @param startDate
     * @param endDate
     */
    private void validateDateParameters(String startDate, String endDate) {
        boolean hasStartDate = startDate != null && !startDate.trim().isEmpty();
        boolean hasEndDate = endDate != null && !endDate.trim().isEmpty();

        if (hasStartDate != hasEndDate) {
            throw new IllegalArgumentException(
                    "Both startDate and endDate must be provided together, or neither should be provided");
        }

        // If both dates are provided, validate them
        if (hasStartDate && hasEndDate) {
            ZonedDateTime start;
            ZonedDateTime end;

            // Parse and validate start date
            try {
                start = LocalDate.parse(startDate.trim()).atStartOfDay(ZoneOffset.UTC);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(
                        "Invalid startDate format. Expected format: YYYY-MM-DD (e.g., 2026-01-15)");
            }

            // Parse and validate end date
            try {
                end = LocalDate.parse(endDate.trim()).atStartOfDay(ZoneOffset.UTC);
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException(
                        "Invalid endDate format. Expected format: YYYY-MM-DD (e.g., 2026-01-15)");
            }

            // Validate start date is before or equal to end date
            if (start.isAfter(end)) {
                throw new IllegalArgumentException(
                        "startDate must be before or equal to endDate. Provided: startDate=" + startDate
                                + ", endDate=" + endDate);
            }
        }
    }

    private String getModelEngineId() {
        if (this.keyValue.containsKey(ReactorKeysEnum.ENGINE.getKey())) {
            Object engineObj = this.keyValue.get(ReactorKeysEnum.ENGINE.getKey());
            if (engineObj instanceof String) {
                return (String) engineObj;
            } else if (engineObj instanceof List) {
                List<?> engineList = (List<?>) engineObj;
                if (!engineList.isEmpty()) {
                    return (String) engineList.get(0); // take first if somehow a list
                }
            }
        }
        return null;
    }

}