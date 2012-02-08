/* File:    $Id$
* Revision: $Revision$
* Author:   $Author$
*
* The Netarchive Suite - Software to harvest and preserve websites
* Copyright 2004-2012 The Royal Danish Library, the Danish State and
 * University Library, the National Library of France and the Austrian
 * National Library.
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 2.1 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this library; if not, write to the Free Software
* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
*/
package dk.netarkivet.common;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import junit.framework.TestCase;

/** Unittestersuite for the CommonSettings class. */
public class CommonSettingsTester extends TestCase {

    @SuppressWarnings("rawtypes")
	public void testNoFinalSettingsConstants() {
        Class c = CommonSettings.class;
        Field[] fields = c.getDeclaredFields();
        for (Field f: fields) {
            // Check that all static public fields are not final
            int modifiers = f.getModifiers();
            if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers)) {
                assertFalse("public static fields must not be final, but this was violated by field " + f.getName(),            
                         Modifier.isFinal(modifiers));
            }
        }
    }
}
