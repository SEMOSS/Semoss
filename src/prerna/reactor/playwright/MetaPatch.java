package prerna.reactor.playwright;

/**
 * Represents a patch for updating metadata, specifically the title,
 * description, and intent. This record is an immutable data carrier used for modifying the
 * metadata of a Playwright recording or session.
 *
 * @param title       The new title for the metadata. If null, the existing
 *                    title will be preserved.
 * @param description The new description for the metadata. If null, the
 *                    existing description will be preserved.
 * @param intent      The new intent for the metadata. If null, the existing
 *                    intent will be preserved.
 */
public record MetaPatch(String title, String description, String intent) {
}

