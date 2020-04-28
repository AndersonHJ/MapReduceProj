package sort;


public class Flight {

	private int airlineID;
	private int year;
	private int month;
	private long depTime;
	private double arrDelayMinutes;

	/**
	 * Origin, Dest, FlightDate, ArrTime, DepTime, Cancelled != 1, Diverted != 1, ArrDelayMinutes, FlightNum,
	 * Year, Month
	 */
	public Flight(int airlineID, int year, int month, long depTime, double arrDelayMinutes) {
		this.airlineID = airlineID;
		this.year = year;
		this.month = month;
		this.depTime = depTime;
		this.arrDelayMinutes = arrDelayMinutes;
	}

	/**
	 * @return the flightNum
	 */
	public int getAirlineID() {
		return airlineID;
	}

	/**
	 * @return the year of flight
	 */
	public int getYear() {
		return year;
	}
	
	/**
	 * @return the year of flight
	 */
	public int getMonth() {
		return month;
	}

	/**
	 * @return the depTime
	 */
	public Long getDepTime() {
		return depTime;
	}

	/**
	 * @return the arrDelayMinutes
	 */
	public Double getArrDelayMinutes() {
		return arrDelayMinutes;
	}
	
	
	public String toString() {
		return "data: " + this.airlineID + "\t" + this.depTime + "\t" + this.year + "\t" + this.month + "\t" + this.arrDelayMinutes;
	}
}
