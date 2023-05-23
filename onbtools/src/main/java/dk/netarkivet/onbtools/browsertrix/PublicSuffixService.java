package dk.netarkivet.onbtools.browsertrix;

import java.util.StringTokenizer;
import java.util.regex.Pattern;

import dk.netarkivet.common.utils.DomainUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PublicSuffixService {
	protected static final Log log = LogFactory.getLog(PublicSuffixService.class);
	
	String hostname;
	String correctedHostname;
	String tld;
	boolean root = true;
	boolean hostnamecorrect = true;
	
	static final String HTTPS = "https://";
	static final String HTTP = "http://";
	static final String FTP = "ftp://";
	static final String DNS = "dns:";
	
//	static final String validHostnameRegex = "^([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9])(\\.([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]{0,61}[a-zA-Z0-9]))*$";
	static final String validHostnameRegex = "(\\A[a-zA-Z0-9])([a-zA-Z0-9\\-]*)([a-zA-Z0-9]\\z)";
	
	public PublicSuffixService(String aUrl) {
		if (aUrl == null) {
			aUrl = "";
		}
		
		hostname = aUrl.trim();
    	
    	if (hostname.startsWith(HTTP)) {
    		hostname = hostname.substring(HTTP.length());
    	}

    	if (hostname.startsWith(HTTPS)) {
    		hostname = hostname.substring(HTTPS.length());
    	}

    	if (hostname.startsWith(FTP)) {
    		hostname = hostname.substring(FTP.length());
    	}

    	if (hostname.startsWith(DNS)) {
    		hostname = hostname.substring(DNS.length());
    	}
    	
    	
    	if (hostname.endsWith("/")) {
    		hostname = hostname.substring(0, hostname.length() - 1);
    	}
    	
    	int idx;
    	
    	idx = hostname.indexOf("/"); 
    	if (idx != -1) {
    		root = false;
    		hostname = hostname.substring(0, idx);
    	}
    	
    	StringTokenizer stk = new StringTokenizer(hostname, "@");
    	if (stk.countTokens() > 1) {
    		while(stk.hasMoreElements()) {
    			hostname = stk.nextToken();
    		}
    	}
    	
    	idx = hostname.lastIndexOf(":"); 
    	if (idx != -1 && idx > 0) {
    		hostname = hostname.substring(0, idx);
    	}
    	
    	Pattern p = Pattern.compile(validHostnameRegex);
    	StringTokenizer st = new StringTokenizer(hostname, ".");
    	String token;
    	correctedHostname = "";
    	while(st.hasMoreTokens()) {
    		token = st.nextToken();

    		if (p.matcher(token).matches() == false) {
    			hostnamecorrect = false;
    			continue;
    		}
			correctedHostname += token + ".";
    	}
    	
    	if (correctedHostname.length() > 0) {
        	correctedHostname = correctedHostname.substring(0, correctedHostname.length() - 1);
    	}
	}
	
	public boolean isRoot() {
		return root;
	}

	public String getHostname() throws IllegalArgumentException {
		if (hostname.length() == 0) {
			throw new IllegalArgumentException("Hostname is invalid"); 
		}
		
		return hostname;
	}
	
	public String getTld() {
		if (tld == null) {
			String domainname = getDomainname();
			
			int start = domainname.lastIndexOf(".");
			tld = domainname.substring(start + 1);
		}
		
		return tld;
	}
	
	public boolean isHostnameCorrect() {
		return hostnamecorrect;
	}

	public String getDomainname() throws IllegalArgumentException {
		String domainname = DomainUtils.domainNameFromHostname(hostname);
		
		if (domainname == null) {
			throw new IllegalArgumentException("Domain is invalid"); 
		}
		
		return domainname;
	}
}
