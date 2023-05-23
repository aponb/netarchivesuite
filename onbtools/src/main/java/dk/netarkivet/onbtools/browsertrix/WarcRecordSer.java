package dk.netarkivet.onbtools.browsertrix;

import java.io.Serializable;
import java.util.Date;

import org.jwat.common.ContentType;
import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.common.Uri;
import org.jwat.warc.WarcDigest;
import org.jwat.warc.WarcRecord;

public class WarcRecordSer implements Serializable{
	public final static String DEFAULT_DENY_MIME_FILTER = "^text/.*";

	String httpHeaderStr;
	String warcTargetUriStr;
	String warcDateStr;
	WarcDigest warcPayloadDigest;
	String warcPayloadDigestStr;
	String warcIpAddress;
	Uri warcRecordIdUri;
	ContentType contentType;
	String warcRecordIdStr;
	Integer warcTypeIdx;
	String httpheader_contentType;
	String httpheader_statusCodeStr;
	Long contentLen;
	Long httpheaderLen;
	Long payloadLen;
    Date warcDate;
	byte[] httpHeaderRaw;
	
	
	public WarcRecordSer(WarcRecord aOriginalRecord, Long aContentLength) {
    	httpHeaderStr = "";
        contentLen = aContentLength;
        httpheaderLen = 0L;
		
        // http header
        HttpHeader httpHeader = aOriginalRecord.getHttpHeader();
        
        if (httpHeader != null) {
        	if (httpHeader.httpVersion != null) {
        		httpHeaderStr += String.format("%s ",  httpHeader.httpVersion); 
        		
        	}
        	if (httpHeader.statusCode != null) {
        		httpHeaderStr += String.format("%d ",  httpHeader.statusCode); 
        		
        	}
        	if (httpHeader.reasonPhrase != null) {
        		httpHeaderStr += String.format("%s ",  httpHeader.reasonPhrase); 
        		
        	}
    		httpHeaderStr += String.format("\n"); 

            for (HeaderLine hl : httpHeader.getHeaderList()) {
                httpHeaderStr += String.format("%s: %s\n", hl.name, hl.value);
            }
            
        	httpheader_contentType = httpHeader.contentType;
            httpheader_statusCodeStr = httpHeader.statusCodeStr;
            
            httpheaderLen = contentLen - httpHeader.getPayloadLength();
            httpHeaderRaw = httpHeader.getHeader();
        }
        
        if (aOriginalRecord.header != null) {
            warcTypeIdx = aOriginalRecord.header.warcTypeIdx;
            warcTargetUriStr = aOriginalRecord.header.warcTargetUriStr;
            warcDateStr = aOriginalRecord.header.warcDateStr;
            warcDate = aOriginalRecord.header.warcDate;
            warcPayloadDigest = aOriginalRecord.header.warcPayloadDigest;
            warcPayloadDigestStr = aOriginalRecord.header.warcPayloadDigestStr; 
            warcIpAddress = aOriginalRecord.header.warcIpAddress;
            
            warcRecordIdUri = aOriginalRecord.header.warcRecordIdUri;
            contentType = aOriginalRecord.header.contentType;
            
            warcRecordIdStr = aOriginalRecord.header.warcRecordIdStr;
        }
        
        payloadLen = contentLen - httpheaderLen;
	}
	
	public boolean isDeduplicationCandidate() {
		if (httpheader_contentType != null && httpheader_contentType.matches(DEFAULT_DENY_MIME_FILTER)) {
			return false;
		}
		
		return true;
	}

	public String getHttpHeaderStr() {
		return httpHeaderStr;
	}

	public String getWarcTargetUriStr() {
		return warcTargetUriStr;
	}

	public String getWarcDateStr() {
		return warcDateStr;
	}

	public WarcDigest getWarcPayloadDigest() {
		return warcPayloadDigest;
	}

	public String getWarcPayloadDigestStr() {
		return warcPayloadDigestStr;
	}

	public String getWarcIpAddress() {
		return warcIpAddress;
	}

	public Uri getWarcRecordIdUri() {
		return warcRecordIdUri;
	}

	public ContentType getContentType() {
		return contentType;
	}

	public String getWarcRecordIdStr() {
		return warcRecordIdStr;
	}

	public Integer getWarcTypeIdx() {
		return warcTypeIdx;
	}

	public String getHttpheader_contentType() {
		return httpheader_contentType;
	}

	public String getHttpheader_statusCodeStr() {
		return httpheader_statusCodeStr;
	}

	public Long getPayloadLen() {
		return payloadLen;
	}

	public Long getContentLen() {
		return contentLen;
	}

	public Long getHttpheaderLen() {
		return httpheaderLen;
	}
	
	public Date getWarcDate() {
		return warcDate;
	}
	
    public byte[] getHttpHeaderRaw() {
		return httpHeaderRaw;
	}
}
