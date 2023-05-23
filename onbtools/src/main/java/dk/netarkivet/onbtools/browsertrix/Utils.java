package dk.netarkivet.onbtools.browsertrix;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Utils {
	protected static final Log log = LogFactory.getLog(Utils.class);

    public static final int NUMBEROFRETRIES = 144;		// 12 Stunden
    public static final int WAITINGTIME = 5 * 60 * 1000; // 5 minutes

	public static DateTimeFormatter TIMESTAMP14 = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
	public static final int TIMESTAMP14_LEN = 14;
	/**
	 * Utility function for creating arc-style date stamps
	 * in the format yyyyMMddHHmmss.
	 * Date stamps are in the UTC time zone
	 *
	 * @param date milliseconds since epoc
	 * @return the date stamp
	 */
	public static String get14DigitDate(Date date){
		return TIMESTAMP14.format(date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime());
	}

	public static Date parse14DigitDate(String str) throws ParseException{
		return Date.from(LocalDateTime.parse(str, TIMESTAMP14).atZone(ZoneId.systemDefault()).toInstant());
	}

	//2023-05-19T18:19:55Z
	// 20230519181955 -> 2023-05-19T18:19:55Z
	public static String convert14DigitDateToWarcDateStr(String str) {
		return
				str.substring(0, 4) +
						"-" + str.substring(4, 6) +
						"-" + str.substring(6, 8) +
						"T" + str.substring(8, 10) +
						":" + str.substring(10, 12) +
						":" + str.substring(12, 14) + "Z";
	}

	public static void appendFile(File aSrcFile, File aDestFile) {
		BufferedReader br = null;
		BufferedWriter fout = null;
		
		try {
			fout = new BufferedWriter(new FileWriter(aDestFile, true));
			br = new BufferedReader(new FileReader(aSrcFile));
			String line = null;
			while ((line = br.readLine()) != null) {
				fout.write(line);
				fout.newLine();
			}
			br.close();
			fout.close();
		} catch (IOException e) {
			if (fout != null) {
				try {
					fout.close();
				} catch (IOException ex) {
				}
			}
			if (br != null) {
				try {
					br.close();
				} catch (IOException ex) {
				}
			}
			
			log.error(e);
		}
	}

	public static String getDomainNameFromSeed(String aSeed) {
		PublicSuffixService service = new PublicSuffixService(aSeed);
		String domainName;

		try {
			domainName = service.getDomainname();
		}
		catch(Exception e) {
			log.error("Invalid domain. Doing nothing.");
			return null;
		}

		return domainName;
	}

	public static void writeLineToFile(String aLine, String aFile) {
		writeLineToFile(aLine, aFile, true);
	}

	public static void writeLineToFile(String aLine, String aFile, boolean aAppend) {
		BufferedWriter fout = null;

		try {
			fout = new BufferedWriter(new FileWriter(aFile, aAppend));

			fout.write(aLine);
			fout.newLine();
			fout.close();

		} catch (IOException e2) {
			if (fout != null) {
				try {
					fout.close();
				} catch (IOException e3) {
				}
			}
			log.error(e2);
		}
	}

	public static PropertiesConfiguration getPropertiesConfiguration(String aPropertiesfilename) throws ConfigurationException {
		FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
				new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class)
						.configure(new Parameters().properties()
								.setBasePath(System.getProperty("user.dir"))
								.setListDelimiterHandler(new DefaultListDelimiterHandler(','))
								.setFileName(aPropertiesfilename));
		return builder.getConfiguration();
	}
}
