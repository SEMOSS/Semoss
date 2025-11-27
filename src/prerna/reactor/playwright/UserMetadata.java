package prerna.reactor.playwright;

/**
 * Represents basic metadata about a user. This record is an immutable data
 * carrier for user identification and location information.
 *
 * @param name  The user's name.
 * @param email The user's email address.
 * @param id    A unique identifier for the user.
 * @param zone  The user's geographical or logical zone.
 */
public record UserMetadata(String name, String email, String id, String zone) {
}
