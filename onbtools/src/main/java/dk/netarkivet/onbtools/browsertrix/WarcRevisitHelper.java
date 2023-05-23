package dk.netarkivet.onbtools.browsertrix;

import java.io.Serializable;

public class WarcRevisitHelper implements Serializable{
	public WarcRecordSer getWrs() {
		return wrs;
	}

	public void setWrs(WarcRecordSer wrs) {
		this.wrs = wrs;
	}

	public String getDedupLogLine() {
		return dedupLogLine;
	}

	public void setDedupLogLine(String dedupLogLine) {
		this.dedupLogLine = dedupLogLine;
	}

	public String getRefersToDateStr() {
		return refersToDateStr;
	}

	public void setRefersToDateStr(String refersToDateStr) {
		this.refersToDateStr = refersToDateStr;
	}

	public String getRefersToUrl() {
		return refersToUrl;
	}

	public void setRefersToUrl(String refersToUrl) {
		this.refersToUrl = refersToUrl;
	}

	WarcRecordSer wrs;
	String dedupLogLine;
	String refersToDateStr;
	String refersToUrl;

	public WarcRevisitHelper() {
	}
}
