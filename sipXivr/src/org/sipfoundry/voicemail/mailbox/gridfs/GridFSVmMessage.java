package org.sipfoundry.voicemail.mailbox.gridfs;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.sipfoundry.voicemail.mailbox.Folder;
import org.sipfoundry.voicemail.mailbox.MessageDescriptor;
import org.sipfoundry.voicemail.mailbox.VmMessage;

public class GridFSVmMessage extends VmMessage {
	
    public GridFSVmMessage(String messageId, File audioFile, MessageDescriptor descriptor, boolean urgent) {
		super(messageId, audioFile, descriptor, urgent);
	}

	public GridFSVmMessage(String messageId, String username, File audioFile, MessageDescriptor descriptor,
			Folder folder, boolean unheard, boolean urgent) {
		super(messageId, username, audioFile, descriptor, folder, unheard, urgent);
	}
	
    @Override
    public void cleanup() {
        if(getAudioFile() != null) {
            FileUtils.deleteQuietly(getAudioFile());
        }
    }
	
	
}
