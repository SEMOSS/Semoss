package prerna.util;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** A server-clock snapshot whose UTC and local values describe the same instant. */
public record RuntimeTimeContext(Instant now, ZoneId timeZone) {

	public RuntimeTimeContext {
		Objects.requireNonNull(now, "now");
		Objects.requireNonNull(timeZone, "timeZone");
	}

	public static RuntimeTimeContext capture(ZoneId timeZone) {
		return new RuntimeTimeContext(Instant.now(), timeZone == null ? ZoneId.of("UTC") : timeZone);
	}

	/** Invalid or missing profile zones fall back to the authenticated user's zone, then UTC. */
	public static ZoneId resolveZone(String profileZone, ZoneId userZone) {
		if (profileZone != null && !profileZone.isBlank()) {
			try {
				return ZoneId.of(profileZone.trim());
			} catch (DateTimeException e) {
				// A stored label may not be a valid Java timezone identifier.
			}
		}
		return userZone == null ? ZoneId.of("UTC") : userZone;
	}

	public Map<String, Object> toMap() {
		ZonedDateTime local = now.atZone(timeZone);
		Map<String, Object> values = new LinkedHashMap<>();
		values.put("nowUtc", now.toString());
		values.put("localDateTime", local.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
		values.put("today", local.toLocalDate().toString());
		values.put("weekday", local.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH));
		values.put("timeZone", timeZone.getId());
		return values;
	}

	/** Dynamic data belongs at the conversation tail, never in the cached system prefix. */
	public String guidance() {
		Map<String, Object> values = toMap();
		return "Current time from the SEMOSS server clock: UTC=" + values.get("nowUtc")
				+ "; local=" + values.get("localDateTime") + "; today=" + values.get("today")
				+ " (" + values.get("weekday") + "); timezone=" + values.get("timeZone")
				+ ". Use this latest clock rather than earlier runtime clocks.";
	}
}
