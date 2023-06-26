package dk.netarkivet.onbtools.browsertrix;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

public class ProgressCalculator {
	private int totalNumber;
	private int currentNumber = 0;
	private int skippingNumber = 0;
	
	public static int UNKNOWN = -1; 
	public static String NA = "n.a."; 

    private static final ThreadLocal<DecimalFormat> FORMATTER =
	    new ThreadLocal<DecimalFormat>() {
	        @Override
	        protected DecimalFormat initialValue() {
	        	DecimalFormat f = new DecimalFormat("#.00");
	        	DecimalFormatSymbols dfs = f.getDecimalFormatSymbols();
	    	    dfs.setDecimalSeparator('.');
	        	f.setDecimalFormatSymbols(dfs);
	            return f;
	        }
	    };    

	public ProgressCalculator(int aTotalNumber) {
		this(aTotalNumber, 0);
	}

	public ProgressCalculator(int aTotalNumber, int aSkippingNumber) {
		this.totalNumber = aTotalNumber;
		this.skippingNumber = aSkippingNumber;
		
		if (skippingNumber < 0) {
			skippingNumber = 0;
		}
		
		if (skippingNumber > aTotalNumber) {
			skippingNumber = aTotalNumber;
		}
		
		currentNumber = skippingNumber;
	}

	public double getNextPercentRounded(int aPlaces) {
		return Utils.round(getNextPercent(), aPlaces);
	}

	public double getNextPercentRounded() {
		return getNextPercentRounded(0);
	}
	
	protected synchronized void increment() {
		currentNumber++;			
	}
	
	public int getCurrentNumber() {
		return currentNumber;
	}
	
	public double getNextPercent() {
		increment();
		return (getPercent(currentNumber));
	}

	public double getPercent(int aCurrentNumber) {
		if (totalNumber == 0) {
			return UNKNOWN;
		}
		currentNumber = aCurrentNumber;
		return (currentNumber * 100.0) / totalNumber;
	}
	
	public double getCurrentPercent() {
		return getPercent(currentNumber);
	}
	
	public String getCurrentStatus() {
		double percent = getCurrentPercent();
		if (percent == UNKNOWN) {
			return NA;
		}
		
		return FORMATTER.get().format(percent) + " % (" + currentNumber + "/" + totalNumber + ")";
	}
}
