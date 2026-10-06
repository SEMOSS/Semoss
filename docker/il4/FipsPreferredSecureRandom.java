import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

/**
 * A SecureRandom that resolves, by provider name, to whichever FIPS-approved
 * randomness source this deployment already pins for session IDs - for
 * callers that can only name a public no-arg-constructible class, not a
 * provider or algorithm. Tomcat's {@code CsrfPreventionFilterBase} is the
 * motivating case: its {@code randomClass} init-param is instantiated via
 * {@code Class.forName(name).getConstructor().newInstance()}, so there is no
 * way to hand it a provider or algorithm directly.
 *
 * <p>
 * In the default (unnamed) package and compiled standalone, like
 * RdsTrust.java/FipsCheck.java/AccpCheck.java, deliberately NOT part of the
 * org.semoss:semoss application source: {@code CsrfPreventionFilterBase}'s
 * single-argument {@code Class.forName(name)} resolves using the CALLER's own
 * classloader (Tomcat's shared/common one, since that class lives in
 * tomcat-catalina.jar), not the webapp's - a class bundled inside Monolith's
 * WAR is invisible to it no matter how it's packaged there. This class is
 * instead compiled directly into Tomcat's own {@code $CATALINA_HOME/lib/},
 * where the common classloader can actually find it (confirmed by
 * decompiling the real tomcat-catalina-11.0.26.jar and, the first time
 * around, by reproducing the exact ClassNotFoundException locally).
 *
 * <p>
 * Tries, in order, the exact (algorithm, provider) pairs the two IL4
 * variants' own {@code conf/context.xml} {@code <Manager>} elements already
 * pin for session IDs - BCFIPS/DEFAULT, then
 * AmazonCorrettoCryptoProvider/LibCryptoRng - and falls back to the platform
 * default {@code SecureRandom} when neither provider is registered (a
 * non-IL4 SEMOSS deployment).
 */
public final class FipsPreferredSecureRandom extends SecureRandom {

	private static final long serialVersionUID = 1L;

	private static final String[][] PREFERRED = { { "DEFAULT", "BCFIPS" },
			{ "LibCryptoRng", "AmazonCorrettoCryptoProvider" } };

	// Resolved once per JVM: every caller gets the same deployment-wide answer,
	// and there is no point repeating the lookup for each new instance.
	private static final SecureRandom DELEGATE = resolve();

	private static SecureRandom resolve() {
		for (String[] pair : PREFERRED) {
			try {
				return SecureRandom.getInstance(pair[0], pair[1]);
			} catch (NoSuchAlgorithmException | NoSuchProviderException e) {
				// Not registered on this deployment; try the next preferred source.
			}
		}
		return null;
	}

	@Override
	public void nextBytes(byte[] bytes) {
		if (DELEGATE != null) {
			DELEGATE.nextBytes(bytes);
		} else {
			super.nextBytes(bytes);
		}
	}

	// next(int) is declared final on SecureRandom and always calls nextBytes()
	// (confirmed via javap against the actual JDK class), so every method built
	// on it - nextInt(), nextLong(), nextBoolean(), nextDouble() - already goes
	// through the override above. generateSeed() is not, so it needs its own
	// override; unused by Tomcat's CSRF filter today, kept for any future caller
	// that does call it.
	@Override
	public byte[] generateSeed(int numBytes) {
		return DELEGATE != null ? DELEGATE.generateSeed(numBytes) : super.generateSeed(numBytes);
	}
}
