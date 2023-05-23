package dk.netarkivet.onbtools.browsertrix;

import java.io.File;

public class FileInfo {
	public FileInfo() {
	}
	
	public FileInfo(File aFile) {
		this(aFile, 0, 0);
	}

	public FileInfo(File aFile, long aObjects, long aBytes) {
		file = aFile;
		objects = aObjects;
		bytes = aBytes;
	}

	public void addBytes(long aBytes) {
		bytes += aBytes;
	}

	public void addObjects(long aObjects) {
		objects += aObjects;
	}

	public void incrementObjects() {
		objects++;
	}
	
	public long getObjects() {
		return objects;
	}
	public void setObjects(long objects) {
		this.objects = objects;
	}
	public long getBytes() {
		return bytes;
	}
	public void setBytes(long bytes) {
		this.bytes = bytes;
	}
	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}
	
	long objects = 0;
	long bytes = 0;
	File file;
}
