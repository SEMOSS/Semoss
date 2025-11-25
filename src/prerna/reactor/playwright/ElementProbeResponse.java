package prerna.reactor.playwright;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Represents a comprehensive response containing detailed information about a
 * probed HTML element. This record is designed to provide a snapshot of an
 * element's properties for analysis or interaction. Fields with null values
 * will be omitted during JSON serialization due to
 * {@code @JsonInclude(JsonInclude.Include.NON_NULL)}.
 *
 * @param tag              The HTML tag name of the element (e.g., "input",
 *                         "textarea", "button", "a").
 * @param type             The type attribute of the element (e.g., "text",
 *                         "password", "submit" for input elements).
 * @param inputCategory    A categorization of the input type (e.g., "text",
 *                         "option", "boolean", "action", "link").
 * @param role             The ARIA role of the element, if present.
 * @param selector         A simple CSS selector synthesized for the element.
 * @param placeholder      The placeholder text of the element.
 * @param labelText        The associated label text, derived from
 *                         {@code <label for=>}, {@code aria-label}, or nearest
 *                         text.
 * @param value            The current value of the element (for
 *                         inputs/textareas).
 * @param href             The href attribute value, if the element is a link.
 * @param contentEditable  True if the element is content editable, false
 *                         otherwise.
 * @param rect             The viewport-relative bounding rectangle of the
 *                         element in CSS pixels.
 * @param metrics          Layout metrics of the element that ignore transforms
 *                         (integers).
 * @param styles           Resolved computed styles for visual cloning.
 * @param placeholderStyle Computed style of the {@code ::placeholder}
 *                         pseudo-element, if supported.
 * @param attrs            HTML attributes relevant to behavior and
 *                         accessibility.
 * @param isTextControl    Convenience flag indicating if the element is a text
 *                         input control.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record ElementProbeResponse(String tag, String type, String inputCategory, String role, String selector,
		String placeholder, String labelText, String value, String href, boolean contentEditable, ElementRect rect,
		ElementMetrics metrics, Map<String, String> styles, Map<String, String> placeholderStyle,
		Map<String, String> attrs, boolean isTextControl) {
}
