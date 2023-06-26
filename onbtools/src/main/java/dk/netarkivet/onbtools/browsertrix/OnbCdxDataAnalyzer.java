package dk.netarkivet.onbtools.browsertrix;

import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OnbCdxDataAnalyzer {
    protected static final Log log = LogFactory.getLog(OnbCdxDataAnalyzer.class);
	
    String line = null;
    boolean valid = false;
	
	String warcrecordid;

	public String getWarcrecordid() {
		return warcrecordid;
	}
	public void setWarcrecordid(String warcrecordid) {
		this.warcrecordid = warcrecordid;
	}

    public OnbCdxDataAnalyzer(String aLine) {
        line = aLine;
    }

    public boolean analyze() {
        if (line == null) {
            return false;
        }

        try {
	        StringTokenizer st2column = new StringTokenizer(line, " ");
			warcrecordid = "-";
			try {
				warcrecordid = st2column.nextToken();
			}
			catch(Exception e) {
				log.debug("no warcrecordid found");
			}
        } catch(Exception e) {
        	log.error("Unexpected error: " + e.getMessage());
        	return false;
        }
        valid = true;
        return true;
    }
    
	public boolean isValid() {
		return valid;
	}
	
	public String getLine() {
		return line;
	}
}
