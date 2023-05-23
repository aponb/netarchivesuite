package dk.netarkivet.onbtools.browsertrix;

public class JobInfo {
	public static final int NEW = 0;
	public static final int SUBMITTED = 1;
	public static final int STARTED = 2;
	public static final int DONE = 3;
	public static final int FAILED = 4;
	public static final int RESUBMITTED = 5;
	public static final int FAILED_REJECTED = 6;

	public long getJobid() {
		return jobid;
	}

	public void setJobid(long jobid) {
		this.jobid = jobid;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	long jobid;
	int status;
}
