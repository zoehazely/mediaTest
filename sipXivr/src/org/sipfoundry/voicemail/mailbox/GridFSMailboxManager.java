package org.sipfoundry.voicemail.mailbox;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.bson.types.ObjectId;
import org.sipfoundry.commons.ivr.MimeType;
import org.sipfoundry.commons.userdb.User;
import org.sipfoundry.commons.userdb.User.EmailFormats;
import org.sipfoundry.voicemail.mailbox.gridfs.GridFSSequenceCounter;
import org.sipfoundry.voicemail.mailbox.gridfs.GridFSVmTemplate;
import org.sipfoundry.voicemail.mailbox.gridfs.VmAudioIdentifier;
import org.springframework.util.Assert;

import com.mongodb.DBObject;
import com.mongodb.MongoException;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

public class GridFSMailboxManager extends AbstractMailboxManager {
	
	private static final String MESSAGEID_COUNTER_KEY_FORMAT = "%04dMSGID";
	private static final String RECORDER_LABEL = "RECORDER";
	private static final String RECORDER_MESSAGE_ID = "RECORDER-MSGID";
	private static final String GREETINGS_LABEL = "GREETINGS";
	
	private GridFSSequenceCounter m_messageIdSequenceCounter;
	private GridFSVmTemplate m_gridFSVmTemplate;
	
	public void init() {
        File mailstore = new File(m_mailstoreDirectory);
        if (!mailstore.exists()) {
            mailstore.mkdir();
        }
    }

	@Override
	public MailboxDetails getMailboxDetails(String username) {
		List<String> inboxMessages = m_gridFSVmTemplate.findMessageIdByLabel(username, Folder.INBOX.getId());
		List<String> savedMessages = m_gridFSVmTemplate.findMessageIdByLabel(username, Folder.SAVED.getId());
		List<String> conferenceMessages = m_gridFSVmTemplate.findMessageIdByLabel(username, Folder.CONFERENCE.getId());
		List<String> deletedMessages = m_gridFSVmTemplate.findMessageIdByLabel(username, Folder.DELETED.getId());
		List<String> unheardMessages = m_gridFSVmTemplate.findMessageIdByLabel(username, Folder.INBOX.getId(), true);
		
		return new MailboxDetails(username, inboxMessages, savedMessages, deletedMessages, conferenceMessages,
                unheardMessages);
	}

	@Override
	public List<VmMessage> getMessages(String username, Folder folder) {
	    List<DBObject> vmMetadatas = m_gridFSVmTemplate.findByLabel(username, folder.getId());

	    List<VmMessage> results = new ArrayList<VmMessage>();
        for (DBObject vmMetadata : vmMetadatas) {
            List<GridFSDBFile> dbFiles = m_gridFSVmTemplate.findFilesByVmId(vmMetadata);
            GridFSDBFile dbFile = selectPreferredAudioIdentifier(dbFiles);
            results.add(m_gridFSVmTemplate.createVmMessage(dbFile.getMetaData(), vmMetadata, null));
        }
        
        return results;
	}

	@Override
	public VmMessage getVmMessage(String username, String messageId, boolean loadAudio) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(username, messageId);
	    return getVmMessage(vmMetadata, loadAudio);
	}

	@Override
	public VmMessage getVmMessage(String username, Folder folder, String messageId, boolean loadAudio) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(username, folder.getId(), messageId);
	    return getVmMessage(vmMetadata, loadAudio);
	}

	@Override
	public void markMessageHeard(User user, VmMessage message) {
	    markMessageHeard(user, message.getMessageId());
	}
	
	@Override
    public void markMessageHeard(User user, String messageId) {
	    markMessageAs(user, messageId, false);
    }
	
	@Override
    public void markMessageUnheard(User user, String messageId) {
	    markMessageAs(user, messageId, true);
    }


	@Override
	public void saveMessage(User user, VmMessage message) {
	    Folder messageFolder = message.getParentFolder();
        if (messageFolder == Folder.SAVED) {
            return;
        }
        
        boolean sendMwi = false;
        if (messageFolder == Folder.INBOX) {
            m_gridFSVmTemplate.move(user, message.getMessageId(), Folder.SAVED.getId(), true);
            sendMwi = true;
        } else if(messageFolder == Folder.DELETED) {
            m_gridFSVmTemplate.move(user, message.getMessageId(), Folder.INBOX.getId());
            sendMwi = true;
        }
        
        if (sendMwi) {
            m_mwi.sendMWI(user, getMailboxDetails(user.getUserName()));
        }
	}

	@Override
	public void deleteMessage(User user, VmMessage message) {
	    Folder messageFolder = message.getParentFolder();
	    if (messageFolder == Folder.DELETED) {
	        DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(user, messageFolder.getId()
	                                                                     , message.getMessageId());
            m_gridFSVmTemplate.delete(vmMetadata);
        } else if (messageFolder == Folder.INBOX || messageFolder == Folder.SAVED) {
            m_gridFSVmTemplate.move(user, message.getMessageId(), Folder.DELETED.getId(), true);
            if (messageFolder == Folder.INBOX) {
                m_mwi.sendMWI(user, getMailboxDetails(user.getUserName()));
            }
        }
	}

	@Override
	public void removeDeletedMessages(String username) {
		List<DBObject> vmMetadatas = m_gridFSVmTemplate.findByLabel(username, Folder.DELETED.getId());
		for(DBObject vmMetadata : vmMetadatas) {
		    m_gridFSVmTemplate.delete(vmMetadata);;
		}
	}

	@Override
	public File getRecordedName(String username) {
	    //Special label for storing/getting recorded file
        DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(username, RECORDER_LABEL, RECORDER_MESSAGE_ID);
        GridFSDBFile dbFile = m_gridFSVmTemplate.findFileByFilename(vmMetadata, getNameFile());
        if(dbFile == null) {
            dbFile = m_gridFSVmTemplate.findFileByFilename(vmMetadata, getAltNameFile());
        }
        
        File recordedName = null;
        try {
            if (dbFile != null) {
                TempMessage tempMessage = createTempMessage(username, "", true);
                recordedName = new File(tempMessage.getTempPath());
                dbFile.writeTo(recordedName);
            } else {
                recordedName = new File(getUserDirectory(username), getNameFile());
                if(!recordedName.exists()) {
                    recordedName = new File(getUserDirectory(username), getAltNameFile());
                }
            }
        } catch (IOException ex) {
            LOG.error("Unable to retrieve recorded message: " + ex.getMessage(), ex);
        } 
        
        return recordedName;
	}

	@Override
	public void saveRecordedName(TempMessage message) {
	    //Special label for storing/getting recorded file
        DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(message.getCurrentUser(), RECORDER_LABEL, RECORDER_MESSAGE_ID);
        GridFSDBFile dbFile = m_gridFSVmTemplate.findFileByFilename(vmMetadata, getNameFile());
        
        try (FileInputStream inputStream = new FileInputStream(message.getTempPath())){
	        if(dbFile != null) {
	            m_gridFSVmTemplate.delete(dbFile);
	        }
	        
	        m_gridFSVmTemplate.store(inputStream, getNameFile(), getAudioFormat(), VmAudioIdentifier.CURRENT
	                , RECORDER_LABEL, RECORDER_MESSAGE_ID, message);
        } catch (IOException ex) {
            LOG.error("Unable to save recorded message: " + ex.getMessage(), ex);
        }
	}

	@Override
	public void saveCustomAutoattendantPrompt(TempMessage message) {
	    try {
            String aaName = getPromptFile();
            File aaFile = new File(m_promptsDirectory, aaName);
            FileUtils.copyFile(new File(message.getTempPath()), aaFile);
        } catch (IOException ex) {
            LOG.error("Failed to save recorded name", ex);
        }
	}

	@Override
	public void saveGreetingFile(GreetingType type, TempMessage message) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(message.getCurrentUser(), GREETINGS_LABEL, type.getId());
        GridFSDBFile dbFile = vmMetadata != null
                                ? m_gridFSVmTemplate.findFileByFilename(vmMetadata, getGreetingTypeName(type))
                                : null;
        
        try (FileInputStream inputStream = new FileInputStream(message.getTempPath())){
            if(dbFile != null) {
                m_gridFSVmTemplate.delete(dbFile);
            }
            
            m_gridFSVmTemplate.store(inputStream, getGreetingTypeName(type), getAudioFormat(), VmAudioIdentifier.CURRENT
                    , GREETINGS_LABEL, type.getId(), message);
        } catch (IOException ex) {
            LOG.error("Unable to save recorded message: " + ex.getMessage(), ex);
        }
	}

	@Override
	public String getGreetingPath(User user, GreetingType type) {
	    //Special label for storing/getting recorded file
        DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(user.getUserName(), GREETINGS_LABEL, type.getId());
        GridFSDBFile dbFile = m_gridFSVmTemplate.findFileByFilename(vmMetadata, getGreetingTypeName(type));
        if(dbFile == null) {
            dbFile = m_gridFSVmTemplate.findFileByFilename(vmMetadata, getAltGreetingTypeName(type));
        }
        
        File greetingFile = null;
        try {
            if (dbFile != null) {
                TempMessage tempMessage = createTempMessage(user.getUserName(), "", true);
                greetingFile = new File(tempMessage.getTempPath());
                dbFile.writeTo(greetingFile);
            }
        } catch (IOException ex) {
            LOG.error("Unable to retrieve recorded message: " + ex.getMessage(), ex);
        } 
        
        return greetingFile != null ? greetingFile.getPath() : null;
	}

	@Override
	public boolean isMessageUnHeard(User user, String messageId) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(user.getUserName(), messageId);
	    if(vmMetadata != null && vmMetadata.containsField(GridFSVmTemplate.UNHEARD)) {
	        return (Boolean)vmMetadata.get(GridFSVmTemplate.UNHEARD);
	    }
	    
	    return false;
	}

	@Override
	public void deleteMessage(User user, String messageId) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(user.getUserName(), messageId);
		if(vmMetadata != null) {
		    m_gridFSVmTemplate.delete(vmMetadata);
		}
	}

	@Override
	public void updateMessageSubject(User user, String messageId, String subject) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(user.getUserName(), messageId);
        if(vmMetadata != null) {
            vmMetadata.put(GridFSVmTemplate.SUBJECT, subject);
            m_gridFSVmTemplate.storeVM(vmMetadata);
        }
	}

	@Override
	public void moveMessageToFolder(User user, String messageId, String destination) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(user.getUserName(), messageId);
        if(vmMetadata != null) {
            vmMetadata.put(GridFSVmTemplate.LABEL, destination);
            m_gridFSVmTemplate.storeVM(vmMetadata);
        }
	}

	@Override
	public void deleteMailbox(String username) {
	    m_gridFSVmTemplate.delete(username);
	}

	@Override
	public void renameMailbox(User user, String oldUser) {
		m_gridFSVmTemplate.changeUser(user, oldUser);
	}

	@Override
	protected VmMessage saveTempMessageInStorage(User destUser, TempMessage message, MessageDescriptor descriptor,
			Folder storageFolder, String messageId) {
	    File tempAudio = new File(message.getTempPath());
	    
	    GridFSFile dbFile = null;
        try (FileInputStream content = new FileInputStream(tempAudio)){
            String filename = messageId + String.format(VmAudioIdentifier.CURRENT.getFormat(), getAudioFormat());
            dbFile = m_gridFSVmTemplate.store(content, filename, MimeType.getMimeByFormat(getAudioFormat())
                                            , VmAudioIdentifier.CURRENT, storageFolder.getId()
                                            , messageId, destUser, descriptor);
        } catch (MongoException ex) {
            LOG.error("VmMessage::newMessage Mongo Error " + ex.getMessage(), ex);
            return null;
        } catch (IOException e) {
            LOG.error("VmMessage::newMessage error " + e.getMessage());
            return null;
        }
        
        if (storageFolder == Folder.INBOX) {
            m_mwi.sendMWI(destUser, getMailboxDetails(destUser.getUserName()));
        }
        
        LOG.info("VmMessage::newMessage created gridfs message " + dbFile.getId());
        
        ObjectId vmId = (ObjectId)dbFile.getMetaData().get(GridFSVmTemplate.VOICEMAIL_ID);
        return getVmMessage(vmId, destUser, true);
	}

	@Override
	protected VmMessage saveTempMessageInStorage(User destUser, TempMessage message, MessageDescriptor descriptor,
			String messageId) {
	    return saveTempMessageInStorage(destUser, message, descriptor, Folder.INBOX, messageId);
	}

	@Override
	protected File getTempFolder(String username) {
	    return getFolder(username, Folder.DELETED);
	}

	@Override
	protected VmMessage copyMessage(String newMessageId, User destUser, TempMessage message) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(message.getCurrentUser(), message.getSavedMessageId());
	    DBObject newVmMetadata = m_gridFSVmTemplate.copy(vmMetadata, destUser, Folder.INBOX.getId(), newMessageId, "Voice Message " + newMessageId);
	    return getVmMessage(newVmMetadata, destUser, true);
	}

	@Override
	protected VmMessage forwardMessage(VmMessage originalMessage, TempMessage comments, MessageDescriptor descriptor,
			User destUser, String newMessageId) {
	    Assert.notNull(originalMessage);
	    Assert.notNull(comments);
	    
	    if(comments.getTempPath() != null) {
    	    try (FileInputStream content = new FileInputStream(comments.getTempPath())) {
                String filename = newMessageId + String.format(VmAudioIdentifier.CURRENT.getFormat(), getAudioFormat());
                m_gridFSVmTemplate.store(content, filename, MimeType.getMimeByFormat(getAudioFormat())
                                                , VmAudioIdentifier.CURRENT, Folder.INBOX.getId()
                                                , newMessageId, destUser
                                                , copyMediaDescription(descriptor, comments));
            } catch (MongoException | IOException ex) {
                LOG.error("VmMessage::forwardMessage Mongo Error " + ex.getMessage(), ex);
                return null;
            }
	    }
        
        DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(originalMessage.getUserName(), Folder.INBOX.getId()
                , originalMessage.getMessageId());
        
        GridFSDBFile preferredAudio = m_gridFSVmTemplate.findFileByAudioIdentifier(vmMetadata
                , VmAudioIdentifier.COMBINED, VmAudioIdentifier.CURRENT);
        
        if(preferredAudio != null) {
            MessageDescriptor preferredAudioDescriptor = m_gridFSVmTemplate.createMessageDescriptor(preferredAudio);
            
            String originalFilename = newMessageId + String.format(VmAudioIdentifier.ORIGINAL.getFormat(), getAudioFormat());
            m_gridFSVmTemplate.store(preferredAudio.getInputStream(), originalFilename, MimeType.getMimeByFormat(getAudioFormat())
                    , VmAudioIdentifier.ORIGINAL, Folder.INBOX.getId(), newMessageId
                    , destUser, copyMediaDescription(descriptor, preferredAudioDescriptor));
            
            String combinedFilename = newMessageId + String.format(VmAudioIdentifier.COMBINED.getFormat(), getAudioFormat());
            if(comments.getTempPath() != null) {
                File combinedFile = null;
                try {
                    combinedFile = combinedForwardAudio(preferredAudio, comments, destUser);
                    m_gridFSVmTemplate.store(combinedFile, combinedFilename, MimeType.getMimeByFormat(getAudioFormat())
                            , VmAudioIdentifier.COMBINED, Folder.INBOX.getId()
                            , newMessageId, destUser, updateMediaDescription(descriptor, combinedFile));
                } catch(Exception ex) {
                    LOG.error("Unable to generate forward message audio: " + ex.getMessage(), ex);
                    return null;
                } finally {
                    FileUtils.deleteQuietly(combinedFile);
                }
            } else {
                m_gridFSVmTemplate.store(preferredAudio.getInputStream(), combinedFilename
                        , MimeType.getMimeByFormat(getAudioFormat())
                        , VmAudioIdentifier.COMBINED, Folder.INBOX.getId()
                        , newMessageId, destUser, descriptor);
            }
            
            m_mwi.sendMWI(destUser, getMailboxDetails(destUser.getUserName()));
            
            DBObject forwardMetadata = m_gridFSVmTemplate.findByMessageId(destUser.getUserName()
                    , Folder.INBOX.getId(), newMessageId);
            return getVmMessage(forwardMetadata, destUser, true);
        }
        
        return null;
	}

	@Override
	protected String nextMessageId() {
		return nextMessageId(String.format(MESSAGEID_COUNTER_KEY_FORMAT
		        , Integer.parseInt(getIvrIdentity())));
	}
	
	private VmMessage getVmMessage(ObjectId vmId, User destUser, boolean loadAudio) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findById(vmId);
	    return getVmMessage(vmMetadata, destUser, loadAudio);
    }
	
	private VmMessage getVmMessage(DBObject vmMetadata, User destUser, boolean loadAudio) {
        //VmMessage.cleanup is called only if the Emailer process the request (this
        //will only be triggered if the user has defined an email). Don't generated
        //temp files if the User don't have an email address
        if (destUser.getEmailFormat() != EmailFormats.FORMAT_NONE
                || destUser.getAltEmailFormat() != EmailFormats.FORMAT_NONE) {
            return getVmMessage(vmMetadata, loadAudio);    
        }
        
        return null;
    }
	
	private VmMessage getVmMessage(DBObject vmMetadata, boolean loadAudio) {
	    List<GridFSDBFile> dbFiles = m_gridFSVmTemplate.findFilesByVmId(vmMetadata);
        GridFSDBFile dbFile = selectPreferredAudioIdentifier(dbFiles);
	    
        String username = (String)vmMetadata.get(GridFSVmTemplate.USER); 
        
	    TempMessage tempMessage = loadAudio ? createTempMessage(username, "", true) : null;
	    try {
	        return m_gridFSVmTemplate.createVmMessage(dbFile, vmMetadata, tempMessage);
	    } catch(IOException ex) {
	        LOG.error("Unable to create temp audio file", ex);
	    }
	    
	    return m_gridFSVmTemplate.createVmMessage(dbFile.getMetaData(), vmMetadata, null);
    }
	
	private void markMessageAs(User user, String messageId, boolean unheard) {
	    DBObject vmMetadata = m_gridFSVmTemplate.findByMessageId(user.getUserName(), messageId);
	    if(vmMetadata != null) {
	        Boolean currentStatus = (Boolean)vmMetadata.get(GridFSVmTemplate.UNHEARD);
	        if(currentStatus != Boolean.valueOf(unheard)) {
	            vmMetadata.put(GridFSVmTemplate.UNHEARD, unheard);
	            m_gridFSVmTemplate.storeVM(vmMetadata);
	            
	            //notify user
	            m_mwi.sendMWI(user, getMailboxDetails(user.getUserName()));
	        }
	    }
	}
	
	private GridFSDBFile selectPreferredAudioIdentifier(List<GridFSDBFile> dbFiles) {
	    VmAudioIdentifier selectedAudio = null;
	    GridFSDBFile preferredAudio = null;
	    for(GridFSDBFile dbFile : dbFiles) {
	        VmAudioIdentifier currentAudioIdentifier = VmAudioIdentifier.lookUp(
                    (String)dbFile.getMetaData().get(GridFSVmTemplate.AUDIO_IDENTIFIER)
                );
	        
	        if(selectedAudio != null) {
                //Using the natural order defined in VmAudioIdentifier
                //Combined > Current > Original
                if(currentAudioIdentifier.compareTo(selectedAudio) > 0) {
                    selectedAudio = currentAudioIdentifier;
                    preferredAudio = dbFile;
                }
            } else {
                selectedAudio = currentAudioIdentifier;
                preferredAudio = dbFile;
            }
	    }
	    
        return preferredAudio;
    }
	
	private File combinedForwardAudio(GridFSDBFile originalAudio, TempMessage comments, User destUser) 
	        throws Exception {

	    TempMessage original = createTempMessage(destUser.getUserName(), "", true);
	    File originalFile = m_gridFSVmTemplate.createLocalFile(originalAudio, original);
	    
	    try {
	        File combinedFile = new File(createTempMessage(destUser.getUserName(), "", true).getTempPath());
	        concatAudio(combinedFile, originalFile, new File(comments.getTempPath()));
	        return combinedFile;
	    } finally {
	        FileUtils.deleteQuietly(originalFile);
	    }
	}
	
	private MessageDescriptor copyMediaDescription(MessageDescriptor vmDescriptor, MessageDescriptor origDescriptor) {
	    MessageDescriptor descriptor = new MessageDescriptor();
	    descriptor.setId(vmDescriptor.getId());
        descriptor.setFromUri(vmDescriptor.getFromUri());
        descriptor.setSubject(vmDescriptor.getSubject());
        descriptor.setTimestamp(vmDescriptor.getTimestampString());
        descriptor.setPriority(vmDescriptor.getPriority());
        
        if (vmDescriptor.getOtherRecipients() != null) {
            for(String otherDescriptor : vmDescriptor.getOtherRecipients()) {
                descriptor.addOtherRecipient(otherDescriptor);
            }
        }

        descriptor.setDurationSecs(origDescriptor.getDurationSecs());
        descriptor.setFilePath(origDescriptor.getFilePath());
        descriptor.setAudioFormat(origDescriptor.getAudioFormat());
        descriptor.setContentLength(origDescriptor.getContentLength());
        
        return descriptor;
	}
	
	private MessageDescriptor copyMediaDescription(MessageDescriptor vmDescriptor, TempMessage tempMessage) {
	    MessageDescriptor descriptor = new MessageDescriptor();
        descriptor.setId(vmDescriptor.getId());
        descriptor.setFromUri(vmDescriptor.getFromUri());
        descriptor.setSubject(vmDescriptor.getSubject());
        descriptor.setTimestamp(vmDescriptor.getTimestampString());
        descriptor.setPriority(vmDescriptor.getPriority());
        
        if (vmDescriptor.getOtherRecipients() != null) {
            for(String otherDescriptor : vmDescriptor.getOtherRecipients()) {
                descriptor.addOtherRecipient(otherDescriptor);
            }
        }

        descriptor.setDurationSecs(tempMessage.getDuration());
        descriptor.setFilePath(tempMessage.getTempPath());
        descriptor.setContentLength(tempMessage.getContentLength());
        return descriptor;
    }
	
	private MessageDescriptor updateMediaDescription(MessageDescriptor vmDescriptor, File audioFile) {
        MessageDescriptor descriptor = new MessageDescriptor();
        descriptor.setId(vmDescriptor.getId());
        descriptor.setFromUri(vmDescriptor.getFromUri());
        descriptor.setSubject(vmDescriptor.getSubject());
        descriptor.setTimestamp(vmDescriptor.getTimestampString());
        descriptor.setPriority(vmDescriptor.getPriority());
        
        if (vmDescriptor.getOtherRecipients() != null) {
            for(String otherDescriptor : vmDescriptor.getOtherRecipients()) {
                descriptor.addOtherRecipient(otherDescriptor);
            }
        }

        descriptor.setDurationSecs(vmDescriptor.getDurationSecs());
        descriptor.setFilePath(vmDescriptor.getFilePath());
        descriptor.setAudioFormat(vmDescriptor.getAudioFormat());
        descriptor.setContentLength(audioFile.length());
        return descriptor;
    }
	
	private synchronized String nextMessageId(String key) {
		try {
			return m_messageIdSequenceCounter.getNextMessageId(key);
        } catch (Exception e) {
            LOG.error("Message::nextMessageId cannot update collection " + key, e);
            throw new RuntimeException(e);
        }
	}
	
	private File getFolder(String username, Folder folder) {
        File file = new File(getUserDirectory(username), folder.toString());
        if (!file.exists()) {
            file.mkdirs();
        }
        return file;
    }
	
	private File getUserDirectory(String username) {
        return new File(m_mailstoreDirectory + File.separator + username);
    }
	
	public GridFSSequenceCounter getMessageIdSequenceCounter() {
		return m_messageIdSequenceCounter;
	}

	public void setMessageIdSequenceCounter(GridFSSequenceCounter messageIdSequenceCounter) {
		this.m_messageIdSequenceCounter = messageIdSequenceCounter;
	}
	
	public GridFSVmTemplate getGridFSVmTemplate() {
        return m_gridFSVmTemplate;
    }

    public void setGridFSVmTemplate(GridFSVmTemplate gridFSVmTemplate) {
        this.m_gridFSVmTemplate = gridFSVmTemplate;
    }

}
