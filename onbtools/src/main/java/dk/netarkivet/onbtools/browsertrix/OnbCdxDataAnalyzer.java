package dk.netarkivet.onbtools.browsertrix;

import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OnbCdxDataAnalyzer {
    protected static final Log log = LogFactory.getLog(OnbCdxDataAnalyzer.class);
	
    String line = null;
    boolean valid = false;
	
	long contentlength;
	String ipaddress;
	String puid;
	long payloadlength;
	
	public long getContentlength() {
		return contentlength;
	}
	public void setContentlength(long contentlength) {
		this.contentlength = contentlength;
	}
	public String getIpaddress() {
		return ipaddress;
	}
	public void setIpaddress(String ipaddress) {
		this.ipaddress = ipaddress;
	}
	public String getPuid() {
		return puid;
	}
	public void setPuid(String puid) {
		this.puid = puid;
	}
	public long getPayloadlength() {
		return payloadlength;
	}
	public void setPayloadlength(long payloadlength) {
		this.payloadlength = payloadlength;
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
	        contentlength = Long.parseLong(st2column.nextToken());
	        ipaddress = st2column.nextToken();
	        puid = st2column.nextToken();
	        payloadlength = Long.parseLong(st2column.nextToken());
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
