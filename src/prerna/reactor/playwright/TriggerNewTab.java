package prerna.reactor.playwright;

/**
 * Indicates whether a Playwright action is expected to trigger the opening of a
 * new browser tab, and if so, provides the ID of that new tab. This record is
 * an immutable data carrier.
 *
 * @param isTrue A boolean flag indicating if a new tab was triggered.
 * @param tabId  The ID of the newly opened tab, if {@code isTrue} is
 *               {@code true}.
 */
record TriggerNewTab(boolean isTrue, String tabId) {
}