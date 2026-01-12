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
package prerna.junit.reactors;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import prerna.junit.reactors.algorithm.AlgorithmPackageTests;
import prerna.junit.reactors.auth.AuthPackageTests;
import prerna.junit.reactors.aws.AwsPackageTests;
import prerna.junit.reactors.cache.CachePackageTests;
import prerna.junit.reactors.comments.CommentsPackageTests;
import prerna.junit.reactors.configure.ConfigurePackageTests;
import prerna.junit.reactors.date.DatePackageTests;
import prerna.junit.reactors.ds.DsPackageTests;
import prerna.junit.reactors.engine.EnginePackageTests;
import prerna.junit.reactors.forms.FormsPackageTests;
import prerna.junit.reactors.io.IOPackageTests;
import prerna.junit.reactors.math.MathPackageTests;
import prerna.junit.reactors.nameserver.NameServerPackageTests;
import prerna.junit.reactors.notifications.NotificationsPackageTests;
import prerna.junit.reactors.om.OMPackageTests;
import prerna.junit.reactors.poi.POIPackageTests;
import prerna.junit.reactors.project.ProjectPackageTests;
import prerna.junit.reactors.quartz.QuartzPackageTests;
import prerna.junit.reactors.query.QueryPackageTests;
import prerna.junit.reactors.rdf.RDFPackageTests;
import prerna.junit.reactors.rpa.RPAPackageTests;
import prerna.junit.reactors.sabelcc2.Sablecc2PackageTests;
import prerna.junit.reactors.search.SearchPackageTests;
import prerna.junit.reactors.security.SecurityPackageTests;
import prerna.junit.reactors.socket.SocketPackageTests;
import prerna.junit.reactors.solr.SolrPackageTests;
import prerna.junit.reactors.tcp.TCPPackageTests;
import prerna.junit.reactors.test.TestPackageTests;
import prerna.junit.reactors.theme.ThemePackageTests;
import prerna.junit.reactors.ui.UIPackageTests;
import prerna.junit.reactors.usertracking.UserTrackingPackageTests;
import prerna.junit.reactors.util.UtilPackageTests;
import prerna.junit.reactors.wikidata.WikiDataPackageTests;

@RunWith(Suite.class)
@SuiteClasses({ 
	AlgorithmPackageTests.class,
	AuthPackageTests.class,
	AwsPackageTests.class,
	CachePackageTests.class,
	CommentsPackageTests.class,
	ConfigurePackageTests.class,
	DatePackageTests.class,
	DsPackageTests.class,
	EnginePackageTests.class,
	FormsPackageTests.class,
	IOPackageTests.class,
	MathPackageTests.class,
	NameServerPackageTests.class,
	NotificationsPackageTests.class,
	OMPackageTests.class,
	POIPackageTests.class,
	ProjectPackageTests.class,
	QuartzPackageTests.class,
	QueryPackageTests.class,
	RDFPackageTests.class,
	RPAPackageTests.class,
	Sablecc2PackageTests.class,
	SearchPackageTests.class,
	SecurityPackageTests.class,
	SocketPackageTests.class,
	SolrPackageTests.class,
	TCPPackageTests.class,
	TestPackageTests.class,
	ThemePackageTests.class,
	UIPackageTests.class,
	UserTrackingPackageTests.class,
	UtilPackageTests.class,
	WikiDataPackageTests.class
	})
public class UnitTests {

}
