package prerna.reactor.playwright;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record ElementProbeResponse(
        String tag,              // "input", "textarea", "button", "a", ...
        String type,             // e.g. input@type ("text","password","submit"...)
        String inputCategory,    // "text", "option", "boolean", "action", "link", ...
        String role,             // ARIA role if present
        String selector,         // simple selector we synthesize
        String placeholder,
        String labelText,        // <label for=...>, aria-label, or nearest text
        String value,            // current value (for inputs/textarea)
        String href,             // if link
        boolean contentEditable,
        ElementRect rect,        // viewport-relative rect in CSS px

        // layout metrics that ignore transforms (integers)
        ElementMetrics metrics,

        // resolved computed styles for visual cloning
        Map<String, String> styles,

        // computed style of ::placeholder (if supported)
        Map<String, String> placeholderStyle,

        // HTML attributes relevant to behavior and a11y
        Map<String, String> attrs,

        // convenience flag
        boolean isTextControl
) {
}
