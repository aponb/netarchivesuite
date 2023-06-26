package dk.netarkivet.onbtools.browsertrix;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import dk.netarkivet.common.CommonSettings;
import dk.netarkivet.common.utils.batch.BatchLocalFiles;
import dk.netarkivet.common.utils.batch.FileBatchJob;
import dk.netarkivet.wayback.batch.WaybackCDXExtractionWARCBatchJob;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import dk.netarkivet.common.distribute.HTTPRemoteFileRegistry;
import dk.netarkivet.common.distribute.JMSConnectionFactory;
import dk.netarkivet.common.distribute.arcrepository.ArcRepositoryClientFactory;
import dk.netarkivet.common.distribute.arcrepository.HarvesterArcRepositoryClient;
import dk.netarkivet.common.utils.Settings;

public class BrowsertrixWarcRewriter {
	protected static final Log log = LogFactory.getLog(BrowsertrixWarcRewriter.class);
	protected String propertyfilename;

	protected String nasDB_SPECIFICS_CLASS;
	protected String nasDB_USERNAME;
	protected String nasDB_PASSWORD;
	protected String nasDB_BASE_URL;
	protected String nasDB_MACHINE;
	protected String nasDB_PORT;
	protected String nasDB_DIR;

	//DroidIdentification droidIdentification;

	File seedsFile;
	ArrayList<String> seeds = new ArrayList<String>();
	DeduplicationReducationJobManager dedupJobManager = new DeduplicationReducationJobManager();

	BrowsertrixWarcRewriterProperties properties = new BrowsertrixWarcRewriterProperties();
	File jobMappingFile;

	DB dbEnv;
	Map<String, String> cdxLineList;

	private void readConfig() {
		try {
			PropertiesConfiguration config = Utils.getPropertiesConfiguration(propertyfilename);

			properties.setArchiveDir(config.getString("archiveDir"));
			properties.setCrawlsDir(config.getString("crawlsDir"));
			properties.setLogsDir(config.getString("logsDir"));
			properties.setCdxDir(config.getString("cdxDir"));
			properties.setCdxCacheDir(config.getString("cdxCacheDir"));
			properties.setReuseCdx(config.getBoolean("reuseCdx", false));
			properties.setMetadataDir(config.getString("metadataDir"));
			properties.setDestDir(config.getString("destDir"));
			properties.setDumpDir(config.getString("dumpDir"));
			properties.setDedupIndexDir(config.getString("dedupIndexDir"));
			properties.setErrorfile(config.getString("errorfile"));
			properties.setInfofile(config.getString("infofile"));
			properties.setHarvestdefinitionname(config.getString("harvestdefinitionname"));
			properties.setJmsbroker(config.getString("jmsbroker"));
			properties.setJmsport(config.getString("jmsport"));
			properties.setNasenvironmentname(config.getString("nasenvironmentname"));
			properties.setNasuploadapplicationname(config.getString("nasuploadapplicationname"));
			properties.setNasuploadapplicationinstanceid(config.getString("nasuploadapplicationinstanceid"));

			properties.setDryRun(config.getBoolean("dryRun", false));
			properties.setUseDeduplication(config.getBoolean("useDeduplication", true));
			if (properties.isUseDeduplication()) {
				properties.setDoDeduplicationForRedirects(config.getBoolean("doDeduplicationForRedirects", false));
			}
			else {
				properties.setDoDeduplicationForRedirects(false);
			}

			properties.setMaxwarcfilesize(config.getLong("maxwarcfilesize", 1073741824));

			if (!properties.isDryRun()) {
				readNasConfig();
			}


		} catch (ConfigurationException e) {
			log.error(e.getMessage());
			throw new RuntimeException("Init error: " + e.getMessage());
		}
	}

	protected void readNasConfig() {
		try {
			PropertiesConfiguration config = Utils.getPropertiesConfiguration(propertyfilename);

			nasDB_SPECIFICS_CLASS = config.getString("nasDB_SPECIFICS_CLASS");
			if (nasDB_SPECIFICS_CLASS == null) {
				throw new ConfigurationException("nasDB_SPECIFICS_CLASS is null");
			}
			log.info("nasDB_SPECIFICS_CLASS:" + nasDB_SPECIFICS_CLASS);

			nasDB_USERNAME = config.getString("nasDB_USERNAME");
			if (nasDB_USERNAME == null) {
				throw new ConfigurationException("nasDB_USERNAME is null");
			}
			log.info("nasDB_USERNAME:" + nasDB_USERNAME);

			nasDB_PASSWORD = config.getString("nasDB_PASSWORD");
			if (nasDB_PASSWORD == null) {
				throw new ConfigurationException("nasDB_PASSWORD is null");
			}
			log.info("nasDB_PASSWORD:" + nasDB_PASSWORD);

			nasDB_BASE_URL = config.getString("nasDB_BASE_URL");
			if (nasDB_BASE_URL == null) {
				throw new ConfigurationException("nasDB_BASE_URL is null");
			}
			log.info("nasDB_BASE_URL:" + nasDB_BASE_URL);

			nasDB_MACHINE = config.getString("nasDB_MACHINE");
			if (nasDB_MACHINE == null) {
				throw new ConfigurationException("nasDB_MACHINE is null");
			}
			log.info("nasDB_MACHINE:" + nasDB_MACHINE);

			nasDB_PORT = config.getString("nasDB_PORT");
			if (nasDB_PORT == null) {
				throw new ConfigurationException("nasDB_PORT is null");
			}
			log.info("nasDB_PORT:" + nasDB_PORT);

			nasDB_DIR = config.getString("nasDB_DIR");
			if (nasDB_DIR == null) {
				throw new ConfigurationException("nasDB_DIR is null");
			}
			log.info("nasDB_DIR:" + nasDB_DIR);

			Settings.set("settings.harvester.datamodel.domain.nextJobId", "0");
		} catch (ConfigurationException e) {
			throw new RuntimeException("config error: " + e.getMessage());
		}
	}

	protected void initNasSettings() {
		Settings.set(CommonSettings.DB_SPECIFICS_CLASS, nasDB_SPECIFICS_CLASS);
		Settings.set(CommonSettings.DB_USERNAME, nasDB_USERNAME);
		Settings.set(CommonSettings.DB_PASSWORD, nasDB_PASSWORD);
		Settings.set(CommonSettings.DB_BASE_URL, nasDB_BASE_URL);
		Settings.set(CommonSettings.DB_MACHINE, nasDB_MACHINE);
		Settings.set(CommonSettings.DB_PORT, nasDB_PORT);
		Settings.set(CommonSettings.DB_DIR, nasDB_DIR);
	}

	private ArrayList<String> readSeedsFromFile(File aSeedsfile) {
		ArrayList<String> seedlist = new ArrayList<String>();

		LineNumberReader f = null;

		try {
			String line = "";
			f = new LineNumberReader(new FileReader(aSeedsfile));
			while ((line = f.readLine()) != null) {
				if (line == null || line.length() == 0 || line.startsWith("#")) {
					continue;
				}

				seedlist.add(line);
			}

			return seedlist;
		} catch (Exception e) {
			log.error("error reading " + aSeedsfile.getAbsolutePath());
			return null;
		}
		finally {
			if (f != null) {
				try {
					f.close();
				} catch (IOException e) { }
			}
		}
	}

	private JobHdInfo createJob() throws Exception {
		log.info("create job");

		NasSeedManagerProperties nasSeedManagerProps = new NasSeedManagerProperties();

		nasSeedManagerProps.setSeedlist(seeds);
		nasSeedManagerProps.setCreateconfiguration(true);
		nasSeedManagerProps.setEmptyHarvestdefinition(true);
		nasSeedManagerProps.setClearSeedlistBeforeAdding(true);
		nasSeedManagerProps.setConfigurationname("browsertrix");
		nasSeedManagerProps.setSeedlistname("browsertrix");
		nasSeedManagerProps.setAddToHarvestdefinition(true);
		nasSeedManagerProps.setHarvestdefinitionname(properties.getHarvestdefinitionname());
		nasSeedManagerProps.setOrderxmlname("default_orderxml");

		NasSeedManager seedManager = new NasSeedManager(nasSeedManagerProps);
		JobHdInfo jobid = seedManager.createFinishedJobForDomainsWithSeedList();

		log.info("job created with id " + jobid.getJobid());

		return jobid;
	}


	private boolean rewrite() {
		try {
			WarcDirectoryReader reader = new WarcDirectoryReader(properties.getArchiveDir());
			reader.read();
			ArrayList<BrowsertrixJob> joblist = reader.getJobfilelist();

			List<String> mappingList = getJobMappingList();

			File cdxFile = null;

			long dryRunJobId = 1;
			for (int i=0; i < joblist.size(); i++) {
				BrowsertrixJob browsertrixJob = joblist.get(i);

				if (properties.getReuseCdx() && mappingList.size() > i) {
					String[] ids = mappingList.get(i).split(" ");
					cdxFile = new File(properties.getCdxDir(), (ids[1] + ".cdx"));
					if (browsertrixJob.getJobid().equals(ids[0]) && cdxFile.exists()) {
						readCdxIntoCache(cdxFile);
						dedupJobManager.addJob(Long.parseLong(ids[1]));	// ids[1] has the NAS jobid
						log.info("skipping Browsertrixjob " + browsertrixJob.getJobid() + ". Job " + ids[1] + " exists");
						continue;
					}
				}

				JobHdInfo jobinfo;

				if (properties.isDryRun()) {
					jobinfo = new JobHdInfo(dryRunJobId, 1L);
					dryRunJobId++;
				}
				else {
					jobinfo = createJob();
				}

				cdxFile = new File(properties.getCdxDir(), jobinfo.getJobid() + ".cdx");

				WarcRewriter rw = new WarcRewriter(jobinfo.getJobid(), jobinfo.getHarvestdefinitionid(), browsertrixJob, properties, dedupJobManager, cdxLineList);
				rw.work();

				FileInfo total = new FileInfo();

				File tmpCdx = new File(properties.getDumpDir(), "tmpcdx");
				FileUtils.deleteQuietly(cdxFile);

				for (FileInfo fi : rw.getRegeneratedFileList()) {
					log.info("begin generating cdx for " + cdxFile.getAbsolutePath());

					BatchLocalFiles blafWarc = new BatchLocalFiles(new File[] { fi.getFile()});
					FileBatchJob job = new WaybackCDXExtractionWARCONBBatchJob();

			        OutputStream os = new ByteArrayOutputStream();
			        blafWarc.run(job, os);
			        os.flush();

			        FileOutputStream foStream = new FileOutputStream(tmpCdx);
			        ((ByteArrayOutputStream)os).writeTo(foStream);
			        foStream.close();

			        Utils.appendFile(tmpCdx, cdxFile);

					log.info("end generating cdx for " + cdxFile.getAbsolutePath());

					if (!properties.isDryRun()) {
						log.info("uploading " + fi.getFile().getAbsolutePath());

						boolean status = uploadFile(fi.getFile());

						log.info("upload success:" + status);
					}
					total.addBytes(fi.getBytes());
					total.addObjects(fi.getObjects());
				}

				log.info("begin reading " + cdxFile.getAbsolutePath() + " into cache");
				readCdxIntoCache(cdxFile);
				log.info("end reading " + cdxFile.getAbsolutePath() + " into cache");

				String jobMapping = browsertrixJob.getJobid() + " " + jobinfo.getJobid();
				Utils.writeLineToFile(jobMapping, jobMappingFile);

				log.info("append jobmapping:" + jobMapping);

				dedupJobManager.addJob(jobinfo.getJobid());

				//insertHarvestCompletedInfo(aExchange.getNumberofobjects(), aExchange.getNumberofbytes(), seedManager.getDomainConfigurationList().get(0).getID(), aExchange.getJobid(), aExchange.getHarvestid(), new Date());
			}
		}
		catch(Exception e) {
			log.error(e.getMessage());
		}

		return true;
	}

	private void readCdxIntoCache(File aF) {
		LineNumberReader f = null;

		try {
			String line = "";
			f = new LineNumberReader(new FileReader(aF));
			int count = 0;
			while ((line = f.readLine()) != null) {
				String hash = CdxLineAnalyzer.getHashOnly(line);

				cdxLineList.put(hash, line);
				count++;
			}
			log.info(count + " lines read into cdxCache.");
		} catch (Exception e) {
			log.warn("error reading into cdxCache. " + e.getMessage());
			return;
		}
		finally {
			if (f != null) {
				try {
					f.close();
				} catch (IOException e) { }
			}
		}
	}

	private List<String> getJobMappingList() {
		List<String> list = new ArrayList<>();
		try {
			list = Files.readAllLines(jobMappingFile.toPath(), Charset.defaultCharset());
		}
		catch(Exception e) {
			log.error(e.getMessage());
		}

		return list;
	}

	private boolean uploadFile(File aFile) {
		HarvesterArcRepositoryClient arcrep = null;
		try {
			log.info("Connecting to ArcRepository");
			arcrep = ArcRepositoryClientFactory.getHarvesterInstance();
			log.info("Uploading file " + aFile.getAbsolutePath() + " ...");

			arcrep.store(aFile);

			log.info("Uploaded.");

			return true;
		}
		catch(Exception e) {
			log.error(e.getMessage());
		} finally {
			// Close connections
			if (arcrep != null) {
				arcrep.close();
			}
			JMSConnectionFactory.getInstance().cleanup();
		}

		return false;
	}

	private void updateJobdata() {
		//TODO update here Jobdata in NAS when necessary

	}

	public static void main(String[] args) {
		String propertyfilename =  "BrowsertrixWarcBaseRewriter.properties";

		if (args.length == 1) {
			propertyfilename = args[0];
		}

		BrowsertrixWarcRewriter rewriter = new BrowsertrixWarcRewriter(propertyfilename);
		rewriter.work();
	}

	public BrowsertrixWarcRewriter(String aPropertyfilename) {
		propertyfilename = aPropertyfilename;
	}

	protected boolean init() {
		log.info("init begin");

		try {
			if (!properties.validate()) {
				log.error("Property validation error");
				return false;
			}

			if (!properties.isDryRun()) {
				initNasSettings();

				/* nas jms configs */

				Settings.set("settings.common.jms.class", "dk.netarkivet.common.distribute.JMSConnectionSunMQ");
				Settings.set("settings.common.jms.broker", properties.getJmsbroker());

				Settings.set("settings.common.jms.port", properties.getJmsport());
				Settings.set("settings.common.environmentName", properties.getNasenvironmentname());
				Settings.set("settings.common.applicationName", properties.getNasuploadapplicationname());
				Settings.set("settings.common.applicationInstanceId", properties.getNasuploadapplicationinstanceid());
			}

			FileUtils.deleteQuietly(new File(properties.getDumpDir()));
			new File(properties.getDumpDir()).mkdirs();

			File cdxDir = new File(properties.getCdxDir());
			if (!cdxDir.exists()) {
				FileUtils.forceMkdir(cdxDir);
			}

			jobMappingFile = new File(properties.getCdxDir(), "jobmapping.txt");
			if (!properties.getReuseCdx()) {
				FileUtils.deleteQuietly(cdxDir);
				FileUtils.forceMkdir(cdxDir);
			}

			seedsFile = new File(properties.getMetadataDir(), OnbWarcConstants.CONVENTION_SEEDS_FILE);

			seeds = readSeedsFromFile(seedsFile);

			File cdxCacheDir = new File(properties.getCdxCacheDir());
			try {
				FileUtils.deleteDirectory(cdxCacheDir);
				cdxCacheDir.mkdirs();
			} catch (IOException e) {
				log.error(e.getMessage());
			}

			dbEnv = DBMaker.fileDB(new File(cdxCacheDir, "cdxcache.db")).fileMmapEnableIfSupported().closeOnJvmShutdown().make();
			cdxLineList = dbEnv.hashMap("cdxLineList").keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).createOrOpen();

			if (seeds == null) {
				return false;
			}
		} catch (Exception e) {
			log.error(e.getMessage());
			return false;
		}

		return true;
	}

	public void closedbs() {
		if (dbEnv != null) {
			dbEnv.close();
		}
	}

	public void work() {
		try {
			log.info("readConfig");
			readConfig();
			if (init()) {
				log.info("rewrite");
				rewrite();
				log.info("rewrite finished");
			}
			log.info("finished");
		}
		catch(Exception e) {
			log.error(e.getMessage());
		}
		finally {
			closedbs();
			HTTPRemoteFileRegistry.getInstance().cleanup();
		}
	}
}
