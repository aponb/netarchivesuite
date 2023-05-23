package dk.netarkivet.onbtools.browsertrix;

import java.text.NumberFormat;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DedupReport {
    protected static final Log log = LogFactory.getLog(DedupReport.class);
	
    public DedupReport() {
    	
    }
    
    public void increment(long aPayload) {
    	handledNumber++;
    	totalAmount += aPayload;
    }
    
    public void incrementDuplicate(long aPayload) {
    	increment(aPayload);
    	duplicateNumber++;
    	duplicateAmount += aPayload;
    	
    }
    
    // General statistics
    /** Number of URIs that make it through the processors exclusion rules
     *  and are processed by it.
     */
    long handledNumber = 0L;
    long duplicateNumber = 0L;
    
    /** The total amount of data represented by the documents who were deemed
     *  duplicates and excluded from further processing.
     */
    long duplicateAmount = 0L;
    
    /** The total amount of data represented by all the documents processed **/
    long totalAmount = 0L;

    /** Accumulated time spent doing lookups, in nanoseconds. Divide by handledNumber for average lookup time **/
    AtomicLong cumulativeLookupDuration = new AtomicLong(0);	
	public String report() {
        StringBuilder ret = new StringBuilder();
        ret.append("Processor: ");
        ret.append("at.ac.onb.diglib.webarchive.tools.warc.WarcRewriter");
        ret.append("\n");
        ret.append("  Function:          Set revisit profile on records deemed duplicate by hash comparison\n");
        ret.append("  Total handled:     " + handledNumber + "\n");
        ret.append("  Duplicates found:  " + duplicateNumber + " " + getPercentage(duplicateNumber, handledNumber) + "\n");
        ret.append("  Bytes total:       " + totalAmount + " (" + formatBytesForDisplay(totalAmount) + ")\n");
        ret.append("  Bytes duplicate:    " + duplicateAmount + " (" + formatBytesForDisplay(duplicateAmount) + ") " +
        			getPercentage(duplicateAmount, totalAmount) + "\n");
        
    	ret.append("  New (no hits):     " + (handledNumber - duplicateNumber) + "\n");
       	
       	ret.append("\n");
        return ret.toString();
	}
	
	protected static String getPercentage(double portion, double total){
		NumberFormat percentFormat = NumberFormat.getPercentInstance(Locale.ENGLISH);
		percentFormat.setMaximumFractionDigits(1);
		return percentFormat.format(portion/total);
	}
	
    /**
     * Takes a byte size and formats it for display with 'friendly' units. 
     * <p>
     * This involves converting it to the largest unit 
     * (of B, KiB, MiB, GiB, TiB) for which the amount will be > 1.
     * <p>
     * Additionally, at least 2 significant digits are always displayed. 
     * <p>
     * Negative numbers will be returned as '0 B'.
     *
     * @param amount the amount of bytes
     * @return A string containing the amount, properly formated.
     */
    public static String formatBytesForDisplay(long amount) {
        double displayAmount = (double) amount;
        int unitPowerOf1024 = 0; 

        if(amount <= 0){
            return "0 B";
        }
        
        while(displayAmount>=1024 && unitPowerOf1024 < 4) {
            displayAmount = displayAmount / 1024;
            unitPowerOf1024++;
        }
        
        final String[] units = { " B", " KiB", " MiB", " GiB", " TiB" };
        
        // ensure at least 2 significant digits (#.#) for small displayValues
        int fractionDigits = (displayAmount < 10) ? 1 : 0; 
        return doubleToString(displayAmount, fractionDigits, fractionDigits) 
                   + units[unitPowerOf1024];
    }
    
    public static String doubleToString(double val, int maxFractionDigits){
        return doubleToString(val, maxFractionDigits, 0);
    }
    
    private static String doubleToString(double val, int maxFractionDigits, int minFractionDigits) {
        // NumberFormat returns U+FFFD REPLACEMENT CHARACTER for NaN which looks
        // like a bug in the UI
        if (Double.isNaN(val)) {
            return "NaN";
        }
        NumberFormat f = NumberFormat.getNumberInstance(Locale.US); 
        f.setMaximumFractionDigits(maxFractionDigits);
        f.setMinimumFractionDigits(minFractionDigits);
        return f.format(val); 
    }
    
}
