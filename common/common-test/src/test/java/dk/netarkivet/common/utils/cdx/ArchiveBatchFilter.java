/*
 * #%L
 * Netarchivesuite - common - test
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
package dk.netarkivet.common.utils.cdx;

import java.awt.datatransfer.MimeTypeParseException;
import java.io.Serializable;
import java.util.regex.Pattern;

import org.archive.io.ArchiveRecord;

import dk.netarkivet.common.exceptions.NotImplementedException;

/**
 * A filter class for batch entries. Allows testing whether or not to process an entry without loading the entry data
 * first.
 * <p>
 * accept() is given an ARCRecord rather than a ShareableARCRecord to avoid unnecessary reading and copying of data of
 * records not accepted by filter.
 */
@SuppressWarnings({"unused", "serial"})
public abstract class ArchiveBatchFilter implements Serializable {
    /** A default filter: Accepts everything */
    public static final ArchiveBatchFilter NO_FILTER = new ArchiveBatchFilter("NO_FILTER") {
        public boolean accept(ArchiveRecord record) {
            return true;
        }
    };

    private static final String EXCLUDE_FILE_HEADERS_FILEDESC_PREFIX = "filedesc";
    private static final String EXCLUDE_FILE_HEADERS_FILTER_NAME = "EXCLUDE_FILE_HEADERS";

    /** A default filter: Accepts all but the first file */
    public static final ArchiveBatchFilter EXCLUDE_FILE_HEADERS = new ArchiveBatchFilter(
            EXCLUDE_FILE_HEADERS_FILTER_NAME) {
        public boolean accept(ArchiveRecord record) {
            // return !record.getMetaData().getUrl().startsWith(EXCLUDE_FILE_HEADERS_FILEDESC_PREFIX);
            throw new NotImplementedException("This filter has not yet been implemented");
        }
    };

    private static final String EXCLUDE_HTTP_ENTRIES_HTTP_PREFIX = "http:";
    private static final String ONLY_HTTP_ENTRIES_FILTER_NAME = "ONLY_HTTP_ENTRIES";
    public static final ArchiveBatchFilter ONLY_HTTP_ENTRIES = new ArchiveBatchFilter(ONLY_HTTP_ENTRIES_FILTER_NAME) {
        public boolean accept(ArchiveRecord record) {
            // return record.getMetaData().getUrl().startsWith(EXCLUDE_HTTP_ENTRIES_HTTP_PREFIX);
            throw new NotImplementedException("This filter has not yet been implemented");
        }
    };

    private static final String MIMETYPE_BATCH_FILTER_NAME_PREFIX = "MimetypeBatchFilter-";

    private static final String MIMETYPE_REGEXP = "\\w+/\\w+";
    private static final Pattern MIMETYPE_PATTERN = Pattern.compile(MIMETYPE_REGEXP);

    /**
     * Create a new filter with the given name
     *
     * @param name The name of this filter, for debugging mostly.
     */
    protected ArchiveBatchFilter(String name) {
        /* TODO: Either use the name or remove it. */
    }

    /**
     * @param mimetype String denoting the mimetype this filter represents
     * @return a BatchFilter that filters out all ARCRecords, that does not have this mimetype
     * @throws java.awt.datatransfer.MimeTypeParseException (if mimetype is invalid)
     */
    public static ArchiveBatchFilter getMimetypeBatchFilter(final String mimetype) throws MimeTypeParseException {
        if (!mimetypeIsOk(mimetype)) {
            throw new MimeTypeParseException("Mimetype argument '" + mimetype + "' is invalid");
        }

        return new ArchiveBatchFilter(MIMETYPE_BATCH_FILTER_NAME_PREFIX + mimetype) {
            public boolean accept(ArchiveRecord record) {
                return record.getHeader().getMimetype().startsWith(mimetype);
                // return record.getMetaData().getMimetype().startsWith(mimetype);
            }
        };
    }

    /**
     * Check, if a certain mimetype is valid
     *
     * @param mimetype
     * @return boolean true, if mimetype matches word/word, otherwise false
     */
    public static boolean mimetypeIsOk(String mimetype) {
        return MIMETYPE_PATTERN.matcher(mimetype).matches();
    }

    public abstract boolean accept(ArchiveRecord record);
}
