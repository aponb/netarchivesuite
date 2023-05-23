package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.util.Comparator;

public class DefaultFileComparator implements Comparator<File> {
	public DefaultFileComparator() {}

	public int compare(File af1, File af2) {
		return af1.getName().compareTo(af2.getName());			
	}
}
