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
package prerna.collaboration;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import com.google.common.net.InternetDomainName;

// the owner's own organisation: their domain and the Microsoft tenant's verified domains (read at import),
// with any subdomain of those. Nothing is guessed from names or configured by hand.
public final class BrainOrgDomains {

	// sending subdomains of an organisation that the public suffix list does not know about
	private static final Set<String> MAIL_SUBDOMAINS = Set.of("mail", "email", "e", "offers", "news", "info", "mkt",
			"marketing", "notifications", "reply", "promo", "promotions", "campaign", "lists", "bounce");

	private BrainOrgDomains() {
	}

	/** Domains that are the owner's organisation. */
	public static final class Org {
		private final Set<String> domains;

		private Org(Set<String> domains) {
			this.domains = domains;
		}

		/** The domain, or one it is a subdomain of, belongs to the owner's organisation. */
		public boolean isMine(String domain) {
			if (domain == null || domain.isBlank()) {
				return false;
			}
			String d = domain.trim().toLowerCase(Locale.ROOT);
			for (String mine : domains) {
				if (d.equals(mine) || d.endsWith("." + mine)) {
					return true;
				}
			}
			return false;
		}
	}

	/** The owner's organisation from their own domain and the tenant's verified domains. */
	static Org of(Collection<String> verified, String myDomain) {
		Set<String> domains = new LinkedHashSet<>();
		String own = org(myDomain);
		if (own != null) {
			domains.add(own);
		}
		for (String v : verified) {
			if (v != null && !v.isBlank()) {
				domains.add(v.trim().toLowerCase(Locale.ROOT));
			}
		}
		return new Org(domains);
	}

	/** The owner's organisation as saved at the last import. */
	static Org load(String ownerId, String ownerType, String myDomain) {
		String json = CollaborationDbUtils.queryOne("SELECT ORG_DOMAINS_JSON FROM COLLAB_OWNER WHERE OWNER_ID = ? AND "
				+ "OWNER_TYPE = ?", rs -> CollaborationDbUtils.getString(rs, "ORG_DOMAINS_JSON"), ownerId, ownerType);
		return of(json == null ? List.of() : CollaborationDbUtils.toStringList(CollaborationDbUtils.parseList(json)),
				myDomain);
	}

	/** Saves what the directory returned (name, domains); an empty answer keeps the last one. */
	@SuppressWarnings("unchecked")
	static void save(String ownerId, String ownerType, Map<String, Object> organization) {
		if (organization.get("domains") instanceof List<?> domains && !domains.isEmpty()) {
			CollaborationDbUtils.update("UPDATE COLLAB_OWNER SET ORG_NAME = ?, ORG_DOMAINS_JSON = ?, UPDATED_AT = ? "
					+ "WHERE OWNER_ID = ? AND OWNER_TYPE = ?", organization.get("name"),
					CollaborationDbUtils.toJson((List<Object>) domains), CollaborationDbUtils.now(), ownerId, ownerType);
		}
	}

	// the organisation's domain: comms.deloitte.com is deloitte.com; government keeps the agency (fda.hhs.gov)
	static String org(String domain) {
		if (domain == null || domain.isBlank()) {
			return null;
		}
		String d = domain.trim().toLowerCase(Locale.ROOT);
		while (d.indexOf('.') > 0 && d.indexOf('.') < d.lastIndexOf('.')
				&& MAIL_SUBDOMAINS.contains(d.substring(0, d.indexOf('.')))) {
			d = d.substring(d.indexOf('.') + 1);
		}
		if (d.endsWith(".gov") || d.endsWith(".mil")) {
			return d;
		}
		try {
			InternetDomainName name = InternetDomainName.from(d);
			return name.isUnderPublicSuffix() ? name.topPrivateDomain().toString() : d;
		} catch (IllegalArgumentException | IllegalStateException e) {
			return d;
		}
	}
}
