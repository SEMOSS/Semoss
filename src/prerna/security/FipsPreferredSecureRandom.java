/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.security;

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
 * Tries, in order, the exact (algorithm, provider) pairs the two IL4
 * variants' own {@code conf/context.xml} {@code <Manager>} elements already
 * pin for session IDs - BCFIPS/DEFAULT, then
 * AmazonCorrettoCryptoProvider/LibCryptoRng - and falls back to the platform
 * default {@code SecureRandom} when neither provider is registered (a
 * non-IL4 SEMOSS deployment). Unlike {@link BcFipsProvider}, this never needs
 * to fall back to a directly instantiated, unregistered provider instance:
 * both variants already register their own preferred randomness source
 * globally, by name, for session IDs.
 */
public final class FipsPreferredSecureRandom extends SecureRandom {

	private static final long serialVersionUID = 1L;

	private static final String[][] PREFERRED = { { "DEFAULT", "BCFIPS" },
			{ "LibCryptoRng", "AmazonCorrettoCryptoProvider" } };

	private final SecureRandom delegate;

	public FipsPreferredSecureRandom() {
		this.delegate = resolve();
	}

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
		if (delegate != null) {
			delegate.nextBytes(bytes);
		} else {
			super.nextBytes(bytes);
		}
	}
}
