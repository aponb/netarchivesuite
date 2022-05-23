/*
 * #%L
 * Netarchivesuite - common
 * %%
 * Copyright (C) 2005 - 2018 The Royal Danish Library,
 *             the National Library of France and the Austrian National Library.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 2.1 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Lesser Public License for more details.
 * 
 * You should have received a copy of the GNU General Lesser Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/lgpl-2.1.html>.
 * #L%
 */

package dk.netarkivet.common.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dk.netarkivet.common.exceptions.ArgumentNotValid;

/**
 * Onb Free Space Provider returns the number of bytes free
 * Returning 0 if a given path is not writable or if free space is lower than a given minimum free space percentage
 */

public class OnbFreeSpaceProvider implements FreeSpaceProvider {

    /** The error logger we notify about error messages on. */
    private static final Logger log = LoggerFactory.getLogger(OnbFreeSpaceProvider.class);

    /** The default place in classpath where the settings file can be found. */
    private static String DEFAULT_SETTINGS_CLASSPATH = "dk/netarkivet/common/utils/OnbFreeSpaceProviderSettings.xml";

    /*
     * The static initialiser is called when the class is loaded. It will add default values for all settings defined in
     * this class, by loading them from a settings.xml file in classpath.
     */
    static {
        Settings.addDefaultClasspathSettings(DEFAULT_SETTINGS_CLASSPATH);
    }

    /**
     * <b>settings.common.freespaceprovider.minfreespacemode</b>: <br>
     * The setting the mode for Provider
     */
    public static final String FREESPACEPROVIDER_FREESPACEMODE_SETTING = "settings.common.freespaceprovider.freespacemode";
    /**
     * <b>settings.common.freespaceprovider.minfreespacepercentage</b>: <br>
     * The setting for minimum free space percentage
     */
    public static final String FREESPACEPROVIDER_MINFREESPACEPERCENTAGE_SETTING = "settings.common.freespaceprovider.minfreespacepercentage";
    /**
     * <b>settings.common.freespaceprovider.minfreespace</b>: <br>
     * The setting for minimum free space in Bytes
     */
    public static final String FREESPACEPROVIDER_MINFREESPACE_SETTING = "settings.common.freespaceprovider.minfreespace";

    /**
     * The setting the mode for Provider (percent = percentage check, byte = byte check)
     **/
    private static final String FREESPACEPROVIDER_FREESPACEMODE = Settings.get(FREESPACEPROVIDER_FREESPACEMODE_SETTING);
    
    private static final String FREESPACEPROVIDER_FREESPACEMODE_PERCENTAGE = "percent";
    private static final String FREESPACEPROVIDER_FREESPACEMODE_BYTE = "byte";
    /**
     * The minimum free space percentage
     * e.g. 5 or 5.55
     **/
    private static final Double FREESPACEPROVIDER_MINFREESPACEPERCENTAGE = Double.parseDouble(Settings.get(FREESPACEPROVIDER_MINFREESPACEPERCENTAGE_SETTING));
    /**
     * The minimum free space in bytes
     * e.g. 1000000 
     **/
    private static final long FREESPACEPROVIDER_MINFREESPACE = Long.parseLong(Settings.get(FREESPACEPROVIDER_MINFREESPACE_SETTING));

    
    /**
     * Returns the number of bytes free on the file system that the given file resides on. Will return 0 on non-existing
     * files, on read only files and if free space is lower than given freespacepercentage (in freespacemode percent) or 
     * freespace (in freespacemode byte) in settings.
     *
     * @param f a given dir
     * @return the number of bytes free.
     */
    public long getBytesFree(File f) {
        log.debug("getBytesFree without requestedfilesize calling");
        return getBytesFree(f, 0);
    }

    /**
     * Returns the number of bytes free on the file system that the given file resides on. Will return 0 on non-existing
     * files, on read only files and if free space is lower than given freespacepercentage (in freespacemode percent) or
     * freespace (in freespacemode byte) in settings.
     *
     * @param f a given dir
     * @param requestedFilesize filesize of file which is planned to copy to the given dir. If this value is 0, then
     *                          only the bytesFree for the directory will be calculated
     * @return the number of bytes free.
     */
    public long getBytesFree(File f, long requestedFilesize) {
        ArgumentNotValid.checkNotNull(f, "File f");
        if (!f.exists()) {
            log.warn("The file '{}' does not exist. The value 0 returned.", f.getAbsolutePath());
            return 0;
        }

        if (!f.canWrite()) {
            log.warn("The file '{}' is not writeable. The value 0 returned.", f.getAbsolutePath());
            return 0;
        }

        log.debug("requestedFilesize is '{}'", requestedFilesize);
        log.debug("FreeSpaceMode is '{}'", FREESPACEPROVIDER_FREESPACEMODE);

        if (FREESPACEPROVIDER_FREESPACEMODE_PERCENTAGE.equals(FREESPACEPROVIDER_FREESPACEMODE)) {
            long totalspace;
            long usable;

            totalspace = f.getTotalSpace();
            usable = f.getUsableSpace();

            log.debug("Totalspace: " + totalspace);
            log.debug("Usablespace: " + usable);

            long usable_minus_filesize = usable - requestedFilesize;

            log.debug("usable_minus_filesize: " + usable_minus_filesize);

            double freeSpaceInPercent =  100.0 / totalspace * usable_minus_filesize;
            log.debug("minfreespacepercentage is '{}'", FREESPACEPROVIDER_MINFREESPACEPERCENTAGE);
            log.debug("Free space in percent is '{}'", freeSpaceInPercent);

            if (freeSpaceInPercent <= FREESPACEPROVIDER_MINFREESPACEPERCENTAGE) {
                log.warn("Free space on '{}' is lower than '{}' percent. The value 0 returned.", f.getAbsolutePath(), FREESPACEPROVIDER_MINFREESPACEPERCENTAGE);
                return 0;
            }
            else {
                return usable;
            }
        }

        if (FREESPACEPROVIDER_FREESPACEMODE_BYTE.equals(FREESPACEPROVIDER_FREESPACEMODE)) {
            log.debug("minfreespace is '{}'", FREESPACEPROVIDER_MINFREESPACE);
            log.debug("Free space in byte is '{}'", f.getUsableSpace());

            if (f.getUsableSpace() - requestedFilesize < FREESPACEPROVIDER_MINFREESPACE) {
                log.warn("Free space on '{}' is lower than '{}' bytes. The value 0 returned.", f.getAbsolutePath(), FREESPACEPROVIDER_MINFREESPACE);
                return 0;
            }
            else {
                return f.getUsableSpace() - requestedFilesize;
            }
        }

        log.warn("Mode '{}' not valid. The value 0 returned.", FREESPACEPROVIDER_FREESPACEMODE_BYTE);
        return 0;
    }
}
