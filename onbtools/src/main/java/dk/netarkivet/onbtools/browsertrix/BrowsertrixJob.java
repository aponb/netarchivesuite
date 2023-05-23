package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.util.ArrayList;

public class BrowsertrixJob {
	ArrayList<File> filelist = new ArrayList<File>();
	String jobid;
	
	public BrowsertrixJob(String aJobid, ArrayList<File> aFilelist) {
		jobid = aJobid;
		filelist = aFilelist;
	}
	
	public ArrayList<File> getFilelist() {
		return filelist;
	}
	public String getJobid() {
		return jobid;
	}
	
}
