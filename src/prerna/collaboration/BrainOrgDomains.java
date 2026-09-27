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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.google.common.net.InternetDomainName;

import prerna.util.Constants;
import prerna.util.Utility;

// which domains are the owner's own organisation: their domain, its subdomains, and COLLAB_ORG_DOMAINS
public final class BrainOrgDomains {

	// sending subdomains of an organisation that the public suffix list does not know about
	private static final Set<String> MAIL_SUBDOMAINS = Set.of("mail", "email", "e", "offers", "news", "info", "mkt",
			"marketing", "notifications", "reply", "promo", "promotions", "campaign", "lists", "bounce");

	private BrainOrgDomains() {
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

	// the first label of the organisation's domain: deloitte for deloitte.co.uk
	static String label(String domain) {
		String o = org(domain);
		return o == null ? null : o.contains(".") ? o.substring(0, o.indexOf('.')) : o;
	}

	/** True when the domain belongs to the owner's own organisation. */
	public static boolean isMine(String domain, String myDomain) {
		String o = org(domain);
		if (o == null) {
			return false;
		}
		if (o.equals(org(myDomain))) {
			return true;
		}
		for (String entry : configured()) {
			if (entry.endsWith(".*") ? entry.substring(0, entry.length() - 2).equals(label(o)) : entry.equals(o)) {
				return true;
			}
		}
		return false;
	}

	private static List<String> configured() {
		String value = Utility.getDIHelperProperty(Constants.COLLAB_ORG_DOMAINS);
		List<String> out = new ArrayList<>();
		if (value != null) {
			for (String part : value.split(",")) {
				String p = part.trim().toLowerCase(Locale.ROOT);
				if (!p.isEmpty()) {
					out.add(p);
				}
			}
		}
		return out;
	}
}
