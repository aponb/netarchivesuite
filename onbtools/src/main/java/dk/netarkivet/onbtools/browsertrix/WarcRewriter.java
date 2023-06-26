package dk.netarkivet.onbtools.browsertrix;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jwat.common.Diagnosis;
import org.jwat.common.Payload;
import org.jwat.common.Uri;
import org.jwat.warc.WarcConcurrentTo;
import org.jwat.warc.WarcConstants;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.jwat.warc.WarcWriter;
import org.jwat.warc.WarcWriterFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class WarcRewriter extends WarcBaseWriter {
	protected static final Log log = LogFactory.getLog(WarcRewriter.class);

	protected Gson gson = new GsonBuilder().create();

	private static final String crawlDateFormatString = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
	private static final FastDateFormat crawlDateFormat = FastDateFormat.getInstance(crawlDateFormatString);

	WarcReader reader;
	WarcWriter writer;
	ArcFileName destWarc;

	BrowsertrixWarcRewriterProperties rewriterProperties;

	File outFile;
	File outMetaFile;

	File dedupLog;

	File cdxDir;
	File dumpDir;
	File dedupDir;
	File cacheFile;

	String destDir;
	String metadataDir;

	BrowsertrixJob browsertrixJob;
	ArrayList<FileInfo> regeneratedFileList = new ArrayList<FileInfo>();

	InputStream in = null;
	OutputStream out = null;

	DB dbEnv;
	Map<String, byte[]> recordheaderList;
	Map<String, byte[]> dedupRecordList;
	Map<String, byte[]> revisitRecordMappingList;

	// kommt von BrowsertrixWarcRewriter
	Map<String, String> cdxCache;

	CrawlReport crawlReport;
	String deduplicatereductionjobsLine;

	long jobId;

	SeedManager seedManager;

	public void work() {
		init();

		if (rewriterProperties.isUseDeduplication()) {
			buildHeaderList();
			prepareDeduplicationRecords();
		}

		rewrite();
		writeMetadatafile();

		closedbs();
	}

	public WarcRewriter(long aJobId, long aHarvestId, BrowsertrixJob aBrowsertrixJob, BrowsertrixWarcRewriterProperties aRewriterProperties, DeduplicationReducationJobManager aDedupManager, Map<String, String> aCdxCache) {
		rewriterProperties = aRewriterProperties;
		browsertrixJob = aBrowsertrixJob;

		cdxCache = aCdxCache;

		jobId = aJobId;
		long harvestId = aHarvestId;

		int fileId = 0;
		String customName = "browsertrix";
		String extension = ".warc.gz";
		destDir = aRewriterProperties.getDestDir();
		metadataDir = aRewriterProperties.getMetadataDir();

		dedupDir = new File(aRewriterProperties.getDedupIndexDir());
		cdxDir = new File(aRewriterProperties.getCdxDir());

		try {
			FileUtils.deleteDirectory(dedupDir);
			dedupDir.mkdirs();
		} catch (IOException e) {
			log.error(e.getMessage());
		}

		Date firstDate = null;

		try {
			String[] tokens = browsertrixJob.getFilelist().get(0).getName().split("-");
			firstDate = Utils.parse14DigitDate(tokens[1].substring(0, Utils.TIMESTAMP14_LEN));
		}
		catch(Exception e) {
			log.warn("no Browsertrix warc files. Taking current Date for warc filenaming.");
			firstDate = new Date();
		}

		destWarc = new ArcFileName(jobId, harvestId, firstDate, fileId, customName, extension);

		deduplicatereductionjobsLine = aDedupManager.getMetadataInfoLine();

		outMetaFile = new File(destDir, destWarc.generateMetaDataFilename(true));
		dumpDir = new File(aRewriterProperties.getDumpDir());

		dedupLog = new File(dumpDir, "dedup.log");
		FileUtils.deleteQuietly(dedupLog);
		cacheFile = new File(dumpDir, "dedupcache");
	}

	public void init() {
		File seedsFile = new File(metadataDir, OnbWarcConstants.CONVENTION_SEEDS_FILE);
		seedManager = new SeedManager(seedsFile);

		crawlReport = new CrawlReport(seedManager);

		dbEnv = DBMaker.fileDB(new File(dedupDir, "dedup.db")).fileMmapEnableIfSupported().closeOnJvmShutdown().make();

		recordheaderList = dbEnv.hashMap("recordheaderList").keySerializer(Serializer.STRING).valueSerializer(Serializer.BYTE_ARRAY).createOrOpen();
		dedupRecordList = dbEnv.hashMap("dedupRecordList").keySerializer(Serializer.STRING).valueSerializer(Serializer.BYTE_ARRAY).createOrOpen();
		revisitRecordMappingList = dbEnv.hashMap("revisitRecordMappingList").keySerializer(Serializer.STRING).valueSerializer(Serializer.BYTE_ARRAY).createOrOpen();
	}

	public File getOutFile() {
		return outFile;
	}

	public File getOutMetaFile() {
		return outMetaFile;
	}

	public void closedbs() {
		if (dbEnv != null) {
			dbEnv.close();
		}

		FileUtils.deleteQuietly(dedupLog);
	}

	private void buildHeaderList() {
		try {
			ProgressCalculator pc = new ProgressCalculator(browsertrixJob.getFilelist().size());

			for (File f : browsertrixJob.getFilelist()) {
				pc.getNextPercent();
				log.info(pc.getCurrentStatus() + " - builderHeaderList from " + f.getAbsolutePath());
				in = new FileInputStream(f);

				reader = WarcReaderFactory.getReader(in);
				WarcRecord record;

				while ((record = reader.getNextRecord()) != null) {
					// only Response Records can be deduplicated
					if (record.header.warcTypeIdx != WarcConstants.RT_IDX_RESPONSE) {
						continue;
					}

					Payload pl = record.getPayload();

					long contentLength = pl.getTotalLength();
					WarcRecordSer wrser = new WarcRecordSer(record, contentLength);
					String json = gson.toJson(wrser);

					recordheaderList.put(record.header.warcRecordIdStr, json.getBytes("UTF-8"));
				}

				if (reader != null) {
					reader.close();
				}

				if (in != null) {
					in.close();
				}
			}
		}
		catch (Exception e) {
			log.error(e.getMessage());
		}
		finally {
			try {
				if (reader != null) {
					reader.close();
				}

				if (in != null) {
					in.close();
				}
			}
			catch(Exception e2) {
				// Should not happen
				log.error(e2.getMessage());
			}
		}
	}

	private void prepareDeduplicationRecords() {
		log.info("prepare duplicate records begin");
		try {
			log.info("looping over all records. This could take some time to begin ...");

			ProgressCalculator pc = new ProgressCalculator((int)recordheaderList.size());

			Iterator<String> keys = recordheaderList.keySet().iterator();
			while (keys.hasNext()) {
				pc.getNextPercent();
				String key = keys.next();
				byte[] data = recordheaderList.get(key);

				WarcRecordSer wrs = gson.fromJson(new String(data), WarcRecordSer.class);

				if (!wrs.isDeduplicationCandidate(rewriterProperties)) {
					continue;
				}

				log.debug("do Deduplication " + pc.getCurrentStatus());

				String cdxLine = findDuplicate(wrs.getWarcPayloadDigestStr(), wrs.getWarcTargetUriStr());
				// jetzt potentieller deduplication kandidat

				if (cdxLine != null) {
					// add to revisit List
					CdxLineAnalyzer cla = new CdxLineAnalyzer(cdxLine);
					if (cla.analyze()) {
						prepareRevisitRecord(wrs, cla);
						// als data payload len speichern
						//String json = gson.toJson(cla.getOnbCdxDataAnalayzer().getPayloadlength());
					}
				}
			}
		} catch (Exception de) {
			log.error(de.getMessage());
		}

		log.info("prepare duplicate records end");
	}

	// Content-Length in Warc-Record: Http-Header + Payload
	// Content-Length in http-Header: Payload
	private void prepareRevisitRecord(WarcRecordSer aWrs, CdxLineAnalyzer aCla) throws Exception {
		WarcRevisitHelper wrh = new WarcRevisitHelper();

		wrh.setRefersToTargetUrl(aCla.getUrl());
		wrh.setRefersToDateStr(Utils.convert14DigitDateToWarcDateStr(aCla.getTimestamp()));
		wrh.setRefersTo(aCla.getOnbCdxDataAnalayzer().getWarcrecordid());

		// dedupLogLine erzeugen
		// nach https://heritrix.readthedocs.io/en/latest/operating.html#log-files
		// content-size: http-headerlen (from original) + payload len

		String annotations = "duplicate:\"" + aCla.getArcfilename() + "," + aCla.getOffset() + "," + aCla.getTimestamp() + "\",unwritten:identicalDigest,content-size:" + (aWrs.getHttpHeaderRaw().length + aWrs.getPayloadLen());

		String contentType = CdxLineAnalyzer.EMPTY_FIELD;

		if (aWrs.getHttpheader_contentType() != null) {
			int endIdx = aWrs.getHttpheader_contentType().indexOf(" ");
			if (endIdx == -1) {
				contentType = aWrs.getHttpheader_contentType();
			}
			else {
				contentType = aWrs.getHttpheader_contentType().substring(0, endIdx);
				if (contentType.endsWith(";")) {
					contentType = aWrs.getHttpheader_contentType().substring(0, contentType.length() - 1);
				}
			}
		}

		//	format: 2022-04-30T18:01:38.034Z
		String warcdatestr = crawlDateFormat.format(aWrs.getWarcDate());

		String dedupLogLine = 	warcdatestr + " " +								//  0	date
				aWrs.getHttpheader_statusCodeStr() + " " + 					//  1	http status code
				aWrs.getPayloadLen() + " " +								//  2	Document size: The size of the downloaded document in bytes. For HTTP, this is the size of content only. The size excludes the HTTP response headers. For DNS, the size field is the total size for the DNS response.
				aWrs.getWarcTargetUriStr() + " " +							//  3	Target Uri
				"-" + " " + 												//  4	Discovery Path dummy
				"-" + " " + 												//  5	Referrer dummy
				contentType + " " +											//  6 	content type
				"-" + " " + 												//  7	Worker Thread ID dummy
				"-" + " " + 												//  8	Fetch Timestamp dummy
				aWrs.getWarcPayloadDigest() + " " + 						//	9	Digest
				"-" + " " + 												// 10	Sourcetag dummy
				annotations + " "											// 11   Annotations
				;

		wrh.setWrs(aWrs);
		wrh.setDedupLogLine(dedupLogLine);

		String json = gson.toJson(wrh);
		dedupRecordList.put(aWrs.getWarcRecordIdStr(), json.getBytes());
	}


	private WarcRecord createRevisitRecord(WarcRevisitHelper aWrh) throws URISyntaxException {
		WarcRecord warcRecord = WarcRecord.createRecord(writer);

		String httpHeaderStr = aWrh.getWrs().getHttpHeaderStr();

		// warc header
		Uri recordId = new Uri("urn:uuid:" + UUID.randomUUID().toString());
		warcRecord.header.warcRecordIdUri = recordId;
		warcRecord.header.warcTypeStr = "revisit";
		warcRecord.header.warcTargetUriStr = aWrh.getWrs().getWarcTargetUriStr();
		warcRecord.header.warcDateStr = aWrh.getWrs().getWarcDateStr();
		warcRecord.header.warcPayloadDigest = aWrh.getWrs().getWarcPayloadDigest();
		warcRecord.header.warcIpAddress = aWrh.getWrs().getWarcIpAddress();
		warcRecord.header.warcProfileStr = "http://netpreserve.org/warc/1.0/revisit/identical-payload-digest";
		warcRecord.header.warcRefersToTargetUriStr = aWrh.getRefersToTargetUrl();
		warcRecord.header.warcRefersToDateStr = aWrh.getRefersToDateStr();
		warcRecord.header.warcRefersToUri = new Uri(aWrh.getRefersTo());
		warcRecord.header.contentType = aWrh.getWrs().getContentType();
		long httpHeaderLen = (long) httpHeaderStr.length();
		warcRecord.header.contentLength = httpHeaderLen;

		return warcRecord;
	}

	private void openNewWarcFileForWriting() throws Exception {
		outFile = new File(destDir, destWarc.getName());
		out = new FileOutputStream(outFile);
		writer = WarcWriterFactory.getWriter(out, true);
		writer.setExceptionOnContentLengthMismatch(true);
		writeInfoRecord(writer, outFile.getName(), String.valueOf(jobId));
	}

	private void writingDiagnosticsToErrorfile(WarcWriter aW, String aFilename) {
		if (aW == null) {
			return;
		}

		if (aW.diagnostics.hasErrors()) {
			Utils.writeLineToFile(aFilename, rewriterProperties.getErrorfile());
			String message = "";
			for (Diagnosis d: aW.diagnostics.getErrors()) {
				message += "Type:" + d.type + "\n";
				message += "Entity: "+ d.entity + "\n";
				for (String info : d.information) {
					message += "Info: "+ info +  "\n";
				}
			}

			log.error(message);
			Utils.writeLineToFile(message, rewriterProperties.getErrorfile());
		}
	}

	public void rewrite() {
		in = null;
		out = null;

		log.info("rewrite begin");

		boolean changefile = true;
		long byteswritten = 0;

		regeneratedFileList = new ArrayList<FileInfo>();
		FileInfo currentFileInfo = null;

		if (rewriterProperties.isUseDeduplication()) {
			try {
				ProgressCalculator pc = new ProgressCalculator((int)dedupRecordList.size());

				Iterator<String> keys = dedupRecordList.keySet().iterator();
				while (keys.hasNext()) {
					pc.getNextPercent();
					log.debug(pc.getCurrentStatus() + " - writing revisit records to " +  destWarc.getName());
					if (changefile) {
						// if there are revisit records, write these in a first job file
						openNewWarcFileForWriting();

						currentFileInfo = new FileInfo(outFile);
						regeneratedFileList.add(currentFileInfo);
						byteswritten = 0;
						changefile = false;
					}

					String key = keys.next();
					byte[] data = dedupRecordList.get(key);

					WarcRevisitHelper wrh = gson.fromJson(new String(data), WarcRevisitHelper.class);

					WarcRecord wr = createRevisitRecord(wrh);
					writer.writeHeader(wr);

					String json = gson.toJson(wr.header.warcRecordIdUri);
					revisitRecordMappingList.put(key, json.getBytes());

					if (wrh.getWrs().getHttpHeaderStr().length() > 0) {
						writer.writePayload(wrh.getWrs().getHttpHeaderStr().getBytes());
					}
					Utils.writeLineToFile(wrh.getDedupLogLine(), dedupLog.getAbsolutePath());
					currentFileInfo.incrementObjects();
				}
			} catch (Exception de) {
				log.error(de.getMessage());
			} finally {
				if (writer != null) {
					try {
						writer.close();
						writingDiagnosticsToErrorfile(writer, destWarc.getName());
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}


		try {
			ProgressCalculator pc = new ProgressCalculator(browsertrixJob.getFilelist().size());

			changefile = true;
			destWarc.increaseFileId();

			for (File f : browsertrixJob.getFilelist()) {
				pc.getNextPercent();
				log.info(pc.getCurrentStatus() + " - rewrite " + f.getAbsolutePath());

				in = new FileInputStream(f);
				reader = WarcReaderFactory.getReader(in);

				WarcRecord record;

				while ((record = reader.getNextRecord()) != null) {
					if (changefile) {
						openNewWarcFileForWriting();

						currentFileInfo = new FileInfo(outFile);
						regeneratedFileList.add(currentFileInfo);
						byteswritten = 0;
						changefile = false;
					}

					currentFileInfo.incrementObjects();

					if (rewriterProperties.isUseDeduplication()) {
						byte[] data;
						if ((data = dedupRecordList.get(record.header.warcRecordIdStr)) != null) {
							WarcRevisitHelper wrh = gson.fromJson(new String(data), WarcRevisitHelper.class);
							currentFileInfo.addBytes(wrh.getWrs().getPayloadLen());
							crawlReport.add(record.header.warcTargetUriStr, record.header.contentTypeStr, wrh.getWrs().getPayloadLen(), true);
							continue;
						}

						// suche hier nach nicht response records die aber dedupliziert wurden

						if (record.header.warcConcurrentToList.size() > 0) {
							for (WarcConcurrentTo concurrent : record.header.warcConcurrentToList) {
								if ((data = revisitRecordMappingList.get(concurrent.warcConcurrentToStr)) != null) {
									Uri revisitUri = gson.fromJson(new String(data), Uri.class);

									concurrent.warcConcurrentToUri = revisitUri;
									concurrent.warcConcurrentToStr = revisitUri.toString();
								}
							}
						}
					}

					writer.writeHeader(record);
					log.debug("write " + record.header.warcRecordIdStr + " for " + record.header.warcTargetUriStr + " (contentLength: " + record.header.contentLength + ")");

					if (record.hasPayload()) {
						long payloadLenWritten = writer.streamPayload(record.getPayload().getInputStreamComplete());
						log.debug("Payload: " + payloadLenWritten);
						if (payloadLenWritten > 100000000) {
							Utils.writeLineToFile(record.header.warcTargetUriStr + " " + payloadLenWritten, rewriterProperties.getInfofile());
						}
						byteswritten += payloadLenWritten;
						currentFileInfo.addBytes(payloadLenWritten);
						crawlReport.add(record.header.warcTargetUriStr, record.header.contentTypeStr, payloadLenWritten, false);
					}

					if (byteswritten > rewriterProperties.getMaxwarcfilesize()) {
						if (writer != null) {
							writer.close();
							writingDiagnosticsToErrorfile(writer, destWarc.getName());
						}
						changefile = true;
						destWarc.increaseFileId();
					}
				}
			}
		}
		catch(Exception e) {
			log.error(e.getMessage());
		}
		finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		try {
			FileUtils.touch(dedupLog);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



		log.info("rewrite end");
	}

	public void writeMetadatafile() {
		out = null;

		log.info("write metadatafile begin");

		try {
			out = new FileOutputStream(outMetaFile);
			writer = WarcWriterFactory.getWriter(out, true);
			writer.setExceptionOnContentLengthMismatch(false);

			WarcMetaFileWriter wmw = new WarcMetaFileWriter(writer, outMetaFile.getName(), jobId);
			wmw.add(OnbWarcConstants.BROWSERTRIX_POSTDEDUPLICATOR, "text/plain", dedupLog);

			wmw.add(OnbWarcConstants.BROWSERTRIX_SEEDS, "text/plain", seedManager.getSeedsfile());

			File startup = new File(metadataDir, OnbWarcConstants.CONVENTION_BROWSERTRIXSTARTUP_FILE);
			wmw.add(OnbWarcConstants.BROWSERTRIX_STARTUPSCRIPT, "text/x-shellscript", startup);

			if (rewriterProperties.isUseDeduplication()) {
				File dedup_report = new File(dumpDir, "onbbrowsertrixpostdeduplicator-report.txt");
				FileUtils.writeStringToFile(dedup_report, crawlReport.getDedupReport().report(), "UTF-8");
				wmw.add(OnbWarcConstants.BROWSERTRIX_POSTDEDUPLICATOR_REPORT, "text/plain", dedup_report);

				File duplicatereductionjobs_file = new File(dumpDir, "duplicatereductionjobs.txt");
				Utils.writeLineToFile(deduplicatereductionjobsLine, duplicatereductionjobs_file.getAbsolutePath(), false);
				wmw.add(OnbWarcConstants.BROWSERTRIX_DEDUPLICATEREDUCTIONJOBS, "text/plain", duplicatereductionjobs_file);
			}

			if (browsertrixJob.getJobid() != null) {
				CrawllogDirectoryReader clr = null;

				File crawllog_file = new File(dumpDir, "crawl.log");
				// loop over all files in logs directory to find log files
				clr = new CrawllogDirectoryReader(rewriterProperties.getLogsDir());
				clr.addWildcardFilter("crawl-*-" + browsertrixJob.getJobid() + ".log");
				clr.read();
				crawllog_file.delete();
				for (File f: clr.getFileindex()) {
					Utils.appendFile(f, crawllog_file);
				}
				if (clr.getFileindex().size() > 0) {
					wmw.add(OnbWarcConstants.BROWSERTRIX_LOG, "text/plain", crawllog_file);
				}

				File crawllog_yamlfile = new File(dumpDir, "crawl.yaml");
				// loop over all files in crawls directory to find log files
				clr = new CrawllogDirectoryReader(rewriterProperties.getCrawlsDir());
				clr.addWildcardFilter("crawl-*-" + browsertrixJob.getJobid() + ".yaml");
				clr.read();
				crawllog_yamlfile.delete();
				for (File f: clr.getFileindex()) {
					Utils.appendFile(f, crawllog_yamlfile);
				}

				if (clr.getFileindex().size() > 0) {
					wmw.add(OnbWarcConstants.BROWSERTRIX_LOG_YAML, "text/plain", crawllog_yamlfile);
				}
			}

//            File profile = new File(metadataDir, "diepresse.tar.gz");
//            wmw.add(OnbWarcConstants.BROWSERTRIX_BROWSERPROFILE, "application/gzip", profile);

			wmw.write();

			regeneratedFileList.add(new FileInfo(outMetaFile));
			log.info("write metadatafile end");
		}
		catch (Exception e) {
			log.error(e.getMessage());
		}
		finally {
			try {
				if (writer != null) {
					writer.close();
					writingDiagnosticsToErrorfile(writer, outMetaFile.getName());
				}

				if (out != null) {
					out.close();
				}
			}
			catch(Exception e2) {
				// Should not happen
				log.error(e2.getMessage());
			}
		}
	}

	String[] cdxextensions = { "cdx" };

	private String findDuplicate(String aDigest, String aOriginalurl) {
		if (aDigest == null || aOriginalurl == null) {
			return null;
		}

		aDigest = aDigest.replace("sha1:", "");

		log.debug("find original Line for " + aDigest + " / " + aOriginalurl);

		log.debug("checking cdxCache");

		boolean searchForRedirect = false;

		String cachedLine;
		if ((cachedLine = cdxCache.get(aDigest)) != null) {
			if (CdxLineAnalyzer.getResponsecodelOnly(cachedLine).startsWith("3")) {
				log.debug(" is redirect. check url");
				if (CdxLineAnalyzer.getUrlOnly(cachedLine).equals(aOriginalurl)) {
					log.debug("found in cdxCache");
					return cachedLine;
				}
				else {
					// parse in all cdx files. Maybe redirect was overwritten. We need to find the correct one
					searchForRedirect = true;
				}
			}
			else {
				log.debug("found in cdxCache");
				return cachedLine;
			}
		}

		if (searchForRedirect) {
			log.debug("search correct Url for Redirect");
			log.debug("parsing cdx Files now");

			try {
				Collection<File> filelist = (Collection<File>) FileUtils.listFiles(cdxDir, cdxextensions, false);
				Iterator<File> iter = filelist.iterator();

				while (iter.hasNext()) {
					File f = iter.next();

					aDigest = aDigest.replace("sha1:", "");
					CdxLineReader cdxLineReader;

					try {
						cdxLineReader = new CdxLineReader(aDigest, aOriginalurl, f);
					} catch (Exception e) {
						log.error(e.getMessage());
						return null;
					}
					if (cdxLineReader.parsing()) {
						return cdxLineReader.getOriginalLine();
					}
				}
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}

		return null;
	}

	public ArrayList<FileInfo> getRegeneratedFileList() {
		return regeneratedFileList;
	}
}
