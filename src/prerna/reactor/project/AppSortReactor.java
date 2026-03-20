package prerna.reactor.project;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Reactor to sort apps by date in ascending or descending order.
 * Supports sorting by project_date_last_edited field.
 */
public class AppSortReactor extends AbstractReactor {

	private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";
	private static final String DATE_FIELD = "project_date_last_edited";
	private static final String SORT_ORDER_KEY = "sortOrder";
	private static final String APPS_KEY = "apps";

	public AppSortReactor() {
		this.keysToGet = new String[] { SORT_ORDER_KEY, APPS_KEY };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		List<Map<String, Object>> apps = getAppsFromInput();
		
		String sortOrder = getString(SORT_ORDER_KEY);
		if (sortOrder == null || sortOrder.isEmpty()) {
			sortOrder = "desc";
		}

		sortAppsByDate(apps, sortOrder);

		return new NounMetadata(apps, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.PROJECT_INFO);
	}

	@SuppressWarnings("unchecked")
	private List<Map<String, Object>> getAppsFromInput() {
		List appsInput = getList(APPS_KEY);
		if (appsInput == null || appsInput.isEmpty()) {
			return new ArrayList<>();
		}

		List<Map<String, Object>> apps = new ArrayList<>();
		for (Object appObj : appsInput) {
			if (appObj instanceof Map) {
				apps.add((Map<String, Object>) appObj);
			}
		}

		return apps;
	}

	/**
	 * Sorts the apps list by project_date_last_edited.
	 * 
	 * @param apps    The list of app metadata maps to sort
	 * @param sortOrder Either "asc" for ascending (oldest first) or "desc" for descending (newest first)
	 */
	public static void sortAppsByDate(List<Map<String, Object>> apps, String sortOrder) {
		if (apps == null || apps.isEmpty()) {
			return;
		}

		SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
		boolean isDescending = "desc".equalsIgnoreCase(sortOrder);

		apps.sort(new Comparator<Map<String, Object>>() {
			@Override
			public int compare(Map<String, Object> app1, Map<String, Object> app2) {
				try {
					Object dateObj1 = app1.get(DATE_FIELD);
					Object dateObj2 = app2.get(DATE_FIELD);

					// Handle null or empty date values
					if (dateObj1 == null || dateObj1.toString().isEmpty()) {
						return (dateObj2 == null || dateObj2.toString().isEmpty()) ? 0 : 1;
					}
					if (dateObj2 == null || dateObj2.toString().isEmpty()) {
						return -1;
					}

					Date date1 = dateFormat.parse(dateObj1.toString());
					Date date2 = dateFormat.parse(dateObj2.toString());

					int comparison = date1.compareTo(date2);
					return isDescending ? -comparison : comparison;
				} catch (ParseException e) {
					// If parsing fails, treat as equal
					return 0;
				}
			}
		});
	}

	@Override
	public String getReactorDescription() {
		return "Sorts apps by their last edited date in either ascending (oldest first) or descending (latest first) order.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(SORT_ORDER_KEY)) {
			return "The sort order as either 'asc' for ascending (oldest first) or 'desc' for descending (latest first). Defaults to 'desc'.";
		} else if (key.equals(APPS_KEY)) {
			return "The list of app metadata maps to sort";
		}
		return super.getDescriptionForKey(key);
	}
}
