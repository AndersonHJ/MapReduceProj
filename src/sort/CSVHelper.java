package sort;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

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
	public static Flight getValidFlight(String value, int year){
		CSVReader reader = new CSVReader(new StringReader(value));
		
		String[] data = null;
		try {
			data = reader.readNext();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		try{
			int curYear = Integer.parseInt(data[0]);
	
			if(year == curYear
				&& (long)Float.parseFloat(data[41]) != 1
				&& (long)Float.parseFloat(data[43]) != 1){
				
				int airlineId = Integer.parseInt(data[7]);
				int curMonth = Integer.parseInt(data[2]);
				int depTime = Integer.parseInt(data[24]);
				double arrDelay = Double.parseDouble(data[37]);
	
				// int airlineID, long flightDate, long depTime, double arrDelayMinutes
				return new Flight(airlineId, curYear, curMonth, depTime, arrDelay);
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
			return null;
		}
		
		return null;
	}
	
	public static void main(String[] args) {
		CSVHelper helper = new CSVHelper();
		
		try {
			CSVReader reader = new CSVReader(new FileReader("/Users/houjian/Download/hadoopApp/data03info/On_Time_Performance_1988_10.csv"));
			
			String [] data;
			int year = 1988;
		    while ((data = reader.readNext()) != null) {
		    	int curYear = Integer.parseInt(data[0]);
		    	
				if(year == curYear
					&& (long)Float.parseFloat(data[41]) != 1
					&& (long)Float.parseFloat(data[43]) != 1){
					
					int airlineId = Integer.parseInt(data[7]);
					int curMonth = Integer.parseInt(data[2]);
					int depTime = Integer.parseInt(data[35]);
					double arrDelay = Double.parseDouble(data[37]);
		
					// int airlineID, long flightDate, long depTime, double arrDelayMinutes
					// return new Flight(airlineId, curYear, curMonth, depTime, arrDelay);
				}
		    }
		     
		} catch (CsvValidationException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
