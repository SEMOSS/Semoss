package prerna.junit.reactors.usertracking;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import prerna.usertracking.UserTrackingDetails;

public class UserTrackingDetailsTests {
	
	@Test
	public void testUserTrackingDetails() {
		UserTrackingDetails utd = new UserTrackingDetails(
				"ip", "lat", "long", "country", "state", "city");
		
		assertEquals("SessionTrackedDetails [ipAddr=ip, ipLat=lat, ipLong=long, ipCountry=country, ipState=state, ipCity=city]",
				utd.toString());
	}

}
