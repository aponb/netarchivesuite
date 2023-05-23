package dk.netarkivet.onbtools.browsertrix;

import java.io.File;

public class WarcResourceRecordParameter {
	public WarcResourceRecordParameter(String aUri, String aContentType, File aPayloadfile) {
		uri = aUri;
		contentType = aContentType;
		payloadfile = aPayloadfile;
	}
	
	public String getUri() {
		return uri;
	}
	public void setUri(String uri) {
		this.uri = uri;
	}
	public String getContentType() {
		return contentType;
	}
	public void setContentType(String contentType) {
		this.contentType = contentType;
	}
	public File getPayloadfile() {
		return payloadfile;
	}
	public void setPayloadfile(File payloadfile) {
		this.payloadfile = payloadfile;
	}
	String uri;
	String contentType;
	File payloadfile;
}
