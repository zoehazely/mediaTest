/**
 *
 *
 * Copyright (c) 2011 eZuce, Inc. All rights reserved.
 * Contributed to SIPfoundry under a Contributor Agreement
 *
 * This software is free software; you can redistribute it and/or modify it under
 * the terms of the Affero General Public License (AGPL) as published by the
 * Free Software Foundation; either version 3 of the License, or (at your option)
 * any later version.
 *
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more
 * details.
 */
package org.sipfoundry.voicemail.mailbox;

public enum Folder {
    INBOX("inbox"),
    SAVED("saved"),
    DELETED("deleted"),
    CONFERENCE("conference");
	
	private final String id;
	
	private Folder(final String id) {
		this.id = id;
	}
	
	public String getId() {
		return id;
	}

	@Override
	public String toString() {
		return id;
	}
	
	public static Folder lookUp(String name) {
		Folder folder = tryLookUp(name);
		if(folder != null) {
			return folder;
		}
		
		throw new IllegalArgumentException("Invalid Folder Name");
	}
	
	public static Folder tryLookUp(String name) {
		for(Folder folder : values()) {
			if(folder.id.equalsIgnoreCase(name)) {
				return folder;
			}
		}
		
		return null;
	}
}