package prerna.usertracking;

// TODO: Once we hit java 17 this should 1000% be a records class
public class UserTrackingDetails {

	private String ipAddr;
	private String ipLat;
	private String ipLong;
	private String ipCountry;
	private String ipState;
	private String ipCity;
	
	public UserTrackingDetails(
				String ipAddr,
				String ipLat,
				String ipLong,
				String ipCountry,
				String ipState,
				String ipCity
			) {
		this.ipAddr = ipAddr;
		this.ipLat = ipLat;
		this.ipLong = ipLong;
		this.ipCountry = ipCountry;
		this.ipState = ipState;
		this.ipCity = ipCity;		
	}
	
	
	public String getIpAddr() {
		return ipAddr;
	}


	public void setIpAddr(String ipAddr) {
		this.ipAddr = ipAddr;
	}


	public String getIpLat() {
		return ipLat;
	}


	public void setIpLat(String ipLat) {
		this.ipLat = ipLat;
	}


	public String getIpLong() {
		return ipLong;
	}


	public void setIpLong(String ipLong) {
		this.ipLong = ipLong;
	}


	public String getIpCountry() {
		return ipCountry;
	}


	public void setIpCountry(String ipCountry) {
		this.ipCountry = ipCountry;
	}


	public String getIpState() {
		return ipState;
	}


	public void setIpState(String ipState) {
		this.ipState = ipState;
	}


	public String getIpCity() {
		return ipCity;
	}


	public void setIpCity(String ipCity) {
		this.ipCity = ipCity;
	}


	@Override
	public String toString() {
		return "SessionTrackedDetails [ipAddr=" + ipAddr + ", ipLat=" + ipLat + ", ipLong=" + ipLong + ", ipCountry="
				+ ipCountry + ", ipState=" + ipState + ", ipCity=" + ipCity + "]";
	}
	
}
