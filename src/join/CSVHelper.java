package join;

import java.io.StringReader;

import com.opencsv.CSVReader;

/**
 * @author Shiqi Luo
 *
 */
public class CSVHelper {
	
	/**
	 * Origin, Dest, FlightDate, ArrTime, DepTime, Cancelled != 1, Diverted != 1, ArrDelayMinutes, FlightNum,
	 * Year, Month,
	 * 
	 * String flightNum, String origin, String destination, long flightDate, 
	 * long arrTime, long depTime, long arrDelayMinutes
	 */
	public static String checkFlightValidation(String value, int startYear,
									int startMonth, int endYear, int endMonth){
		CSVReader reader = new CSVReader(new StringReader(value));
		
		String[] data = null;
		try {
			data = reader.readNext();
		} catch (Exception e) {
			e.printStackTrace();
		}
		int curYear = Integer.parseInt(data[0]);
		int curMonth = Integer.parseInt(data[2]);
		
		if(startYear <= curYear && endYear >= curYear 
				&& (data[11].toUpperCase().equals("ORD")
					|| data[17].toUpperCase().equals("JFK"))
				&& (long)Float.parseFloat(data[41]) != 1
				&& (long)Float.parseFloat(data[43]) != 1){
			
			if(startYear == curYear){
				if(startMonth > curMonth){
					return null;
				}
			}
			if(endYear == curYear){
				if(endMonth < curMonth){
					return null;
				}
			}
			
			String[] flightDate = data[5].split("-");
			StringBuilder date = new StringBuilder();
			for(String str:flightDate){
				date.append(str);
			}

			return date.toString();
		}
		return null;
	}
	
	public static Flight getFlight(String value, int startYear, int startMonth, int endYear, int endMonth){
		String date = checkFlightValidation(value, startYear, startMonth, endYear, endMonth);

		if(date != null){
			CSVReader reader = new CSVReader(new StringReader(value));
			
			String[] data = null;
			try {
				data = reader.readNext();
			} catch (Exception e) {
				e.printStackTrace();
			}
			// String flightNum, String origin, String destination, long flightDate, long arrTime, long depTime, double arrDelayMinutes
			return new Flight(data[10], data[11].toUpperCase(), data[17].toUpperCase(), 
					Long.parseLong(date.toString()), Long.parseLong(data[35]), Long.parseLong(data[24]), Double.parseDouble(data[37]));
		}
		
		return null;
	}
}
