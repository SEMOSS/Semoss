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

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;

import prerna.util.Constants;
import prerna.util.Utility;

/**
 * The rules on where the platform sends HTTP requests, set by an admin in
 * RDF_Map:
 *
 * <ul>
 * <li>{@link Constants#BLACK_LIST_DOMAINS} lists what is never called: domains
 * and host names, which also cover their sub-domains, and IP addresses and CIDR
 * ranges such as {@code 10.0.0.0/8}, which also cover every host that resolves
 * into them.</li>
 * <li>{@link Constants#WHITE_LIST_DOMAINS} lists the only places that are
 * called: public domains, which cover their sub-domains, and for internal
 * services, host names, IP addresses and CIDR ranges. With no whitelist, every
 * address the blacklist leaves open is called.</li>
 * </ul>
 *
 * The blacklist is checked first, so an entry on both lists is refused.
 * {@link HttpHelperUtility} applies these rules to every request it sends,
 * including each redirect.
 */
public final class UrlAccessRules {

	private static final Logger classLogger = LogManager.getLogger(UrlAccessRules.class);

	private static volatile HostList parsedBlackList = new HostList(null, List.of(), List.of());
	private static volatile HostList parsedWhiteList = new HostList(null, List.of(), List.of());

	private UrlAccessRules() {
	}

	/**
	 * Checks a URL against the blacklist and the whitelist. With neither list
	 * configured every URL passes.
	 *
	 * @param urlString the URL a request is about to be sent to
	 * @throws IllegalArgumentException when the URL is malformed, has no host, or
	 *                                  the lists do not allow its host
	 */
	public static void checkUrl(String urlString) {
		HostList whiteList = whiteList();
		HostList blackList = blackList();
		if (whiteList.isEmpty() && blackList.isEmpty()) {
			return;
		}
		final String host;
		try {
			host = new URI(urlString).getHost();
		} catch (URISyntaxException e) {
			classLogger.error("Failed to validate domain URL: {}", e.getMessage(), e);
			throw new IllegalArgumentException("Invalid URL: " + urlString + ". Detailed message: " + e.getMessage());
		}
		if (host == null || host.isBlank()) {
			throw new IllegalArgumentException("Invalid URL: " + urlString + ". Unable to determine the host.");
		}
		if (!isAllowed(normalize(host), whiteList, blackList)) {
			throw new IllegalArgumentException("You are not allowed to make requests to the URL: " + urlString);
		}
	}

	/**
	 * Checks a host against the blacklist and the whitelist, for a request whose
	 * URL is not at hand, such as a redirect.
	 *
	 * @param host the host a request is about to be sent to
	 * @throws IllegalArgumentException when the lists do not allow the host
	 */
	public static void checkHost(String host) {
		HostList whiteList = whiteList();
		HostList blackList = blackList();
		if (host == null || (whiteList.isEmpty() && blackList.isEmpty())) {
			return;
		}
		if (!isAllowed(normalize(host), whiteList, blackList)) {
			throw new IllegalArgumentException("You are not allowed to make requests to the host: " + host);
		}
	}

	/**
	 * Checks the addresses a host resolved to: none may be in a blacklisted range.
	 * For a request a project's custom reactor makes, none may be a link local or
	 * wildcard address either, which keeps project code off the cloud metadata
	 * service. Localhost is open to project code like any other host; an admin
	 * closes it by blacklisting it.
	 *
	 * @param host          the host that was resolved
	 * @param addresses     what it resolved to
	 * @param projectCaller whether a project's custom reactor is making the request
	 * @throws IllegalArgumentException when an address is not allowed
	 */
	public static void checkAddresses(String host, InetAddress[] addresses, boolean projectCaller) {
		HostList blackList = blackList();
		if (addresses == null || (!projectCaller && blackList.ranges().isEmpty())) {
			return;
		}
		for (InetAddress address : addresses) {
			if (blackList.covers(address)) {
				throw new IllegalArgumentException("You are not allowed to make requests to the host: " + host);
			}
			if (projectCaller && (address.isLinkLocalAddress() || address.isAnyLocalAddress())) {
				throw new IllegalArgumentException(
						"Project reactors are not allowed to make requests to the host: " + host);
			}
		}
	}

	/**
	 * True when a request needs the addresses of its host checked: a blacklisted
	 * range is configured, or a project's custom reactor is making the request.
	 *
	 * @param projectCaller whether a project's custom reactor is making the request
	 * @return true if {@link #checkAddresses(String, InetAddress[], boolean)} has
	 *         anything to check
	 */
	public static boolean checksAddresses(boolean projectCaller) {
		return projectCaller || !blackList().ranges().isEmpty();
	}

	/**
	 * Decides one host: refused when the blacklist covers it, and with a whitelist
	 * configured, refused unless the whitelist covers it. A public host is covered
	 * when its registered domain is listed. An IP address is covered when it is
	 * listed or in a listed range. An internal host name, which has no registered
	 * domain, is covered when it or a parent of it is listed.
	 *
	 * @param host      the host, in lower case
	 * @param whiteList the whitelist, empty for none
	 * @param blackList the blacklist
	 * @return true if a request may be sent to the host
	 */
	private static boolean isAllowed(String host, HostList whiteList, HostList blackList) {
		boolean isAddress = InetAddresses.isInetAddress(host);
		InetAddress address = isAddress ? InetAddresses.forString(host) : null;
		if (isAddress ? blackList.covers(address) : blackList.covers(host)) {
			return false;
		}
		if (whiteList.isEmpty()) {
			return true;
		}
		if (isAddress) {
			return whiteList.covers(address);
		}
		try {
			return whiteList.hosts().contains(InternetDomainName.from(host).topPrivateDomain().toString());
		} catch (IllegalStateException | IllegalArgumentException e) {
			return whiteList.covers(host);
		}
	}

	/**
	 * A host as the lists compare it: lower case, without the brackets of an IPv6
	 * address and without a trailing dot.
	 *
	 * @param host the host of a URL
	 * @return the host to compare
	 */
	private static String normalize(String host) {
		String normalized = host.trim().toLowerCase(Locale.ROOT);
		if (normalized.startsWith("[") && normalized.endsWith("]")) {
			normalized = normalized.substring(1, normalized.length() - 1);
		}
		if (normalized.endsWith(".")) {
			normalized = normalized.substring(0, normalized.length() - 1);
		}
		return normalized;
	}

	/**
	 * The whitelist from RDF_Map, parsed again only when the setting changes.
	 *
	 * @return the whitelist, empty when none is configured
	 */
	private static HostList whiteList() {
		String setting = Utility.getDIHelperProperty(Constants.WHITE_LIST_DOMAINS);
		HostList current = parsedWhiteList;
		if (current.isFor(setting)) {
			return current;
		}
		HostList parsed = HostList.parse(Constants.WHITE_LIST_DOMAINS, setting);
		parsedWhiteList = parsed;
		return parsed;
	}

	/**
	 * The blacklist from RDF_Map, parsed again only when the setting changes.
	 *
	 * @return the blacklist, empty when none is configured
	 */
	private static HostList blackList() {
		String setting = Utility.getDIHelperProperty(Constants.BLACK_LIST_DOMAINS);
		HostList current = parsedBlackList;
		if (current.isFor(setting)) {
			return current;
		}
		HostList parsed = HostList.parse(Constants.BLACK_LIST_DOMAINS, setting);
		parsedBlackList = parsed;
		return parsed;
	}

	/**
	 * A parsed whitelist or blacklist.
	 *
	 * @param setting the RDF_Map value it was parsed from
	 * @param hosts   the listed domains and host names, in lower case
	 * @param ranges  the listed addresses and address ranges
	 */
	private record HostList(String setting, List<String> hosts, List<Range> ranges) {

		/**
		 * Reads a comma separated list of domains, host names, addresses and CIDR
		 * ranges. An entry with a slash that is not a range is ignored.
		 *
		 * @param name    the RDF_Map key, for the log
		 * @param setting the RDF_Map value, or null
		 * @return the parsed list
		 */
		static HostList parse(String name, String setting) {
			List<String> hosts = new ArrayList<>();
			List<Range> ranges = new ArrayList<>();
			if (setting != null) {
				for (String entry : setting.split(",")) {
					String value = entry.trim().toLowerCase(Locale.ROOT);
					if (value.isEmpty()) {
						continue;
					}
					Range range = Range.parse(value);
					if (range != null) {
						ranges.add(range);
					} else if (value.indexOf('/') >= 0) {
						classLogger.warn("Ignoring the {} entry '{}': not an address range", name, value);
					} else {
						hosts.add(normalize(value.startsWith("*.") ? value.substring(2) : value));
					}
				}
			}
			return new HostList(setting, List.copyOf(hosts), List.copyOf(ranges));
		}

		/**
		 * True when this list was parsed from a setting's current value.
		 *
		 * @param current the RDF_Map value now, or null
		 * @return true if the list is up to date
		 */
		boolean isFor(String current) {
			return current == null ? this.setting == null : current.equals(this.setting);
		}

		/**
		 * True when nothing is listed.
		 *
		 * @return true for an empty list
		 */
		boolean isEmpty() {
			return this.hosts.isEmpty() && this.ranges.isEmpty();
		}

		/**
		 * True when a host name is a listed domain or host, or under one.
		 *
		 * @param host the host name, in lower case
		 * @return true if the list covers the host
		 */
		boolean covers(String host) {
			for (String blocked : this.hosts) {
				if (host.equals(blocked) || host.endsWith("." + blocked)) {
					return true;
				}
			}
			return false;
		}

		/**
		 * True when an address is listed or in a listed range.
		 *
		 * @param address the address
		 * @return true if the list covers the address
		 */
		boolean covers(InetAddress address) {
			for (Range range : this.ranges) {
				if (range.contains(address)) {
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * A listed address or address range.
	 *
	 * @param network      the bytes of the range's first address
	 * @param prefixLength how many leading bits an address shares with it
	 */
	private record Range(byte[] network, int prefixLength) {

		/**
		 * Reads a list entry as an address or a CIDR range.
		 *
		 * @param entry the entry, such as {@code 10.0.0.0/8} or {@code 192.168.1.5}
		 * @return the range, or null when the entry is not an address
		 */
		static Range parse(String entry) {
			int slash = entry.indexOf('/');
			String address = slash < 0 ? entry : entry.substring(0, slash);
			if (!InetAddresses.isInetAddress(address)) {
				return null;
			}
			byte[] network = InetAddresses.forString(address).getAddress();
			int prefixLength = network.length * 8;
			if (slash >= 0) {
				try {
					prefixLength = Integer.parseInt(entry.substring(slash + 1));
				} catch (NumberFormatException e) {
					return null;
				}
				if (prefixLength < 0 || prefixLength > network.length * 8) {
					return null;
				}
			}
			return new Range(network, prefixLength);
		}

		/**
		 * True when an address is in this range.
		 *
		 * @param address the address
		 * @return true if its leading bits match the range
		 */
		boolean contains(InetAddress address) {
			byte[] bytes = address.getAddress();
			if (bytes.length != this.network.length) {
				return false;
			}
			int wholeBytes = this.prefixLength / 8;
			for (int i = 0; i < wholeBytes; i++) {
				if (bytes[i] != this.network[i]) {
					return false;
				}
			}
			int remainingBits = this.prefixLength % 8;
			if (remainingBits == 0) {
				return true;
			}
			int mask = 0xFF << (8 - remainingBits) & 0xFF;
			return (bytes[wholeBytes] & mask) == (this.network[wholeBytes] & mask);
		}
	}
}
