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

	public String getRefersToTargetUrl() {
		return refersToTargetUrl;
	}

	public void setRefersToTargetUrl(String refersToTargetUrl) {
		this.refersToTargetUrl = refersToTargetUrl;
	}

	public String getRefersTo() {
		return refersTo;
	}

	public void setRefersTo(String refersTo) {
		this.refersTo = refersTo;
	}

	WarcRecordSer wrs;
	String dedupLogLine;
	String refersToDateStr;
	String refersToTargetUrl;
	String refersTo;

	public WarcRevisitHelper() {
	}
}
