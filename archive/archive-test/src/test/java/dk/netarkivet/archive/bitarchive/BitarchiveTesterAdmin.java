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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import dk.netarkivet.archive.ArchiveSettings;
import dk.netarkivet.common.distribute.TestRemoteFile;
import dk.netarkivet.common.exceptions.PermissionDenied;
import dk.netarkivet.common.utils.Settings;

/**
 * Unit test for Bitarchive API The handling of administrative data is tested
 */
public class BitarchiveTesterAdmin extends BitarchiveTestCase {
    private static String ARC_FILE_NAME = "CorrectTest1.ARC";
    private static File ARC_FILE_DIR = new File(new File(TestInfo.DATA_DIR, "admin"), "originals");
    private static File ARC_FILE = new File(ARC_FILE_DIR, ARC_FILE_NAME);

    private static File ARCHIVE_DIR_1 = new File(TestInfo.WORKING_DIR, "dir1");
    private static File ARCHIVE_DIR_2 = new File(TestInfo.WORKING_DIR, "dir2");

    protected File getOriginalsDir() {
        return ARC_FILE_DIR;
    }

    /**
     * Create Bitarchive
     *
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Verify that uploading a file adds the appropriate record to admin data.
     *
     * @throws FileNotFoundException
     * @throws PermissionDenied
     * @throws IOException
     */
    @Test
    public void testUploadAdminAdded() throws PermissionDenied, IOException {
        BitarchiveAdmin admin = BitarchiveAdmin.getInstance();
        assertNotNull("Must have admin object.", admin);
        if (!admin.hasEnoughSpace()) {
        	System.err.println("Skipping test. Not enough space on disk to perform test");
        	return;
        }
        // check that the record does not exist in the admin data before upload
        BitarchiveARCFile arcfile = admin.lookup(ARC_FILE_NAME);
        assertNull("Lookup should fail before adding file.", arcfile);

        // upload the file and verify that the file now exists
        archive.upload(new TestRemoteFile(ARC_FILE, false, false, false), ARC_FILE_NAME);

        // now verify that admin data updated correctly
        arcfile = admin.lookup(ARC_FILE_NAME);
        assertNotNull("Lookup of added file should succeed after adding.", arcfile);
    }

    /**
     * Verify that a failed upload does not add an admin record
     */
    @Test
    public void testUploadAdminNotAdded() {
        // FIXME: Doesn't work in windows!
        BitarchiveAdmin admin = BitarchiveAdmin.getInstance();
        assertNotNull("Must have admin object.", admin);

        int countrecords = admin.getFiles().length;

        try {
            archive.upload(new TestRemoteFile(new File(ARC_FILE_DIR, "thisfiledoesnotexist.not"), false, false, false),
                    "thisfiledoesnotexist.not");
            fail("Should have thrown exception when adding missing file.");
        } catch (Exception e) {
            // Expected case.
        }

        // verify that no new records added
        assertNull((admin.lookup("thisfiledoesnotexist.not")));
        assertEquals(countrecords, admin.getFiles().length);
    }

    /**
     * Verify that the bitarchive file dirs are all created and writeable after creating admin.
     */
    @Test
    public void testCTOR() {
        Settings.set(ArchiveSettings.BITARCHIVE_SERVER_FILEDIR, ARCHIVE_DIR_1.getAbsolutePath(),
                ARCHIVE_DIR_2.getAbsolutePath());
        archive.close();
        archive = null;
        archive = Bitarchive.getInstance();
        assertTrue(ARCHIVE_DIR_1 + " should exist after creating bitarchive", ARCHIVE_DIR_1.exists());
        assertTrue(ARCHIVE_DIR_2 + " should exist after creating bitarchive", ARCHIVE_DIR_2.exists());
        assertTrue(ARCHIVE_DIR_1 + " should be writeable after creating bitarchive", ARCHIVE_DIR_1.canWrite());
        assertTrue(ARCHIVE_DIR_2 + " should be writeable after creating bitarchive", ARCHIVE_DIR_2.canWrite());
    }

    /** Check that the constructor handles illegal directories correctly. */
    @Test
    public void testCTORErrors() {
        Settings.set(ArchiveSettings.BITARCHIVE_SERVER_FILEDIR, "/foo:bar");
        archive.close();
        archive = null;
        try {
            archive = Bitarchive.getInstance();
            fail("Should fail when given a nonexisting path");
        } catch (PermissionDenied e) {
            // Expected
        }
        Settings.set(ArchiveSettings.BITARCHIVE_SERVER_FILEDIR, "/:");
        try {
            archive = Bitarchive.getInstance();
            fail("Should fail when given a nonwritable path");
        } catch (PermissionDenied e) {
            // Expected
        }
    }
}
