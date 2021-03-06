/*
 * #%L
 * Netarchivesuite - archive - test
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
package dk.netarkivet.archive.bitarchive;

import java.io.File;

/**
 * TestInfo associated with package dk.netarkivet.archive.bitarchive. Contains useful constants.
 */
public class TestInfo {
    static final File DATA_DIR = new File("tests/dk/netarkivet/archive/bitarchive/data");
    static final File ORIGINALS_DIR = new File(DATA_DIR, "originals");
    static final File WORKING_DIR = new File(DATA_DIR, "working");
    static final File FILE_DIR = new File(WORKING_DIR, "filedir");

    static final File BATCH_OUTPUT_FILE = new File(WORKING_DIR, "batch_output.log");

    static String baAppId = "bitArchiveApp_1";

    static final long BITARCHIVE_BATCH_JOB_TIMEOUT = 7 * 24 * 60 * 60 * 1000;
}
