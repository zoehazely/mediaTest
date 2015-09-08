package org.sipfoundry.voicemail.mailbox.gridfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.bson.types.ObjectId;
import org.sipfoundry.commons.mongo.MongoConstants;
import org.sipfoundry.commons.userdb.User;
import org.sipfoundry.voicemail.mailbox.Folder;
import org.sipfoundry.voicemail.mailbox.MessageDescriptor;
import org.sipfoundry.voicemail.mailbox.MessageDescriptor.Priority;
import org.sipfoundry.voicemail.mailbox.TempMessage;
import org.sipfoundry.voicemail.mailbox.VmMessage;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.gridfs.GridFsTemplate;
import org.springframework.util.Assert;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSFile;

public class GridFSVmTemplate {

    public static final String USER = "user";
    public static final String LABEL = "label";
    public static final String UNHEARD = "unheard";
    public static final String USER_URI = "userURI";
    public static final String FROM_URI = "fromURI";
    public static final String MESSAGE_ID = "messageId";
    public static final String DURATION = "duration";
    public static final String TIMESTAMP = "timestamp";
    public static final String SUBJECT = "subject";
    public static final String PRIORITY = "priority";
    public static final String OTHER_RECIPIENTS = "otherRecipients";
    public static final String AUDIO_FORMAT = "audioFormat";
    public static final String AUDIO_IDENTIFIER = "audioIdentifier";
    public static final String FILE_PATH = "filePath";
    public static final String CONTENT_LENGTH = "contentLength";
    public static final String VOICEMAIL_ID = "voicemailId";
    public static final String FILENAME = "filename";

    public final static String DEFAULT_BUCKET = "voicemail";
    public final static String DEFAULT_METADATA = "metadata";

    private final static String METADATA_VOICEMAIL_ID = DEFAULT_METADATA + "." + VOICEMAIL_ID;
    
    private final GridFsTemplate m_gridFSTemplate;
    private final MongoDbFactory m_dbFactory;
    private final String m_bucket;
    
    public GridFSVmTemplate(MongoDbFactory dbFactory) {
        this(dbFactory, DEFAULT_BUCKET);
    }

    public GridFSVmTemplate(MongoDbFactory dbFactory, String bucket) {
        this(dbFactory, getDefaultMongoConverter(dbFactory), bucket);
    }

    public GridFSVmTemplate(MongoDbFactory dbFactory, MongoConverter converter) {
        this(dbFactory, converter, DEFAULT_BUCKET);
    }

    public GridFSVmTemplate(MongoDbFactory dbFactory, MongoConverter converter, String bucket) {
        this.m_gridFSTemplate = new GridFsTemplate(dbFactory, converter, bucket);
        this.m_dbFactory = dbFactory;
        this.m_bucket = bucket;

        createIndexes();
    }
    
    public GridFSFile store(File file, String filename, String contentType
            , VmAudioIdentifier audioIdentifier, String label, String messageId
            , User destUser, MessageDescriptor descriptor) throws IOException {
        try(FileInputStream content = new FileInputStream(file)) {
            return store(content, filename, contentType, audioIdentifier, label, messageId, destUser, descriptor, true);
        }
    }

    public GridFSFile store(InputStream content, String filename, String contentType
            , VmAudioIdentifier audioIdentifier, String label, String messageId
            , User destUser, MessageDescriptor descriptor) {
        return store(content, filename, contentType, audioIdentifier, label, messageId, destUser, descriptor, true);
    }
    
    public GridFSFile store(InputStream content, String filename, String contentType
            , VmAudioIdentifier audioIdentifier, String label, String messageId
            , User destUser, MessageDescriptor descriptor, boolean unheard) {
        Assert.notNull(content);
        
        DBObject vmMetadata = findVM(destUser.getUserName(), label, messageId
                                , new BasicDBObject(MongoConstants.ID, 1));
        
        boolean newVm = false;
        if(vmMetadata == null) {
            newVm = true;
            vmMetadata = createVmMetadata(new BasicDBObject(MongoConstants.ID, new ObjectId())
                                    , audioIdentifier, label, messageId, destUser
                                    , descriptor, unheard);
        }
        
        DBObject fileMetadata = createFileMetadata(new BasicDBObject()
                , (ObjectId)vmMetadata.get(MongoConstants.ID)
                , audioIdentifier, descriptor);

        //Make sure we are able to add the file in gridfs before inserting the VmMetadata
        GridFSFile fsFile = m_gridFSTemplate.store(content, filename, contentType, fileMetadata);
        if(newVm) {
            storeVM(vmMetadata);
        }
        
        return fsFile;
    }
    
    public GridFSFile store(InputStream content, String filename, String contentType
            , VmAudioIdentifier audioIdentifier, String label, String messageId
            , TempMessage tempMessage) {
        return store(content, filename, contentType, audioIdentifier, label, messageId, tempMessage, false);
    }

    public GridFSFile store(InputStream content, String filename, String contentType
            , VmAudioIdentifier audioIdentifier, String label, String messageId
            , TempMessage tempMessage, boolean unheard) {
        Assert.notNull(content);
        
        DBObject vmMetadata = findVM(tempMessage.getCurrentUser(), label, messageId
                                , new BasicDBObject(MongoConstants.ID, 1));
        
        boolean newVm = false;
        if(vmMetadata == null) {
            newVm = true;
            vmMetadata = createVmMetadata(new BasicDBObject(MongoConstants.ID, new ObjectId())
                                    , audioIdentifier, label, messageId, tempMessage, unheard);
        }
        
        DBObject fileMetadata = createFileMetadata(new BasicDBObject()
                , (ObjectId)vmMetadata.get(MongoConstants.ID)
                , audioIdentifier, tempMessage);

        //Make sure we are able to add the file in gridfs before inserting the VmMetadata
        GridFSFile fsFile = m_gridFSTemplate.store(content, filename, contentType, fileMetadata);
        if(newVm) {
            storeVM(vmMetadata);
        }
        
        return fsFile;
    }
    
    public DBObject storeVM(DBObject metadata) {
        doStoreVM(metadata);
        return metadata;
    }
    
    public DBObject copy(DBObject origVm, User destUser, String newLabel
            , String newMessageId, String newSubject) {
        Assert.notNull(origVm);
        Assert.notNull(destUser);
        
        BasicDBObject copyVm = new BasicDBObject();
        copyVm.putAll(origVm);
        copyVm.append(MongoConstants.ID, new ObjectId())
              .append(USER, destUser.getUserName())
              .append(USER_URI, destUser.getIdentity())
              .append(LABEL, newLabel)
              .append(MESSAGE_ID, newMessageId)
              .append(SUBJECT, newSubject)
              .append(UNHEARD, true);
        
        List<GridFSDBFile> origFiles = findFilesByVmId(origVm);
        for(GridFSDBFile origFile : origFiles) {
            DBObject currentMetadata = origFile.getMetaData();
            String newFilename = StringUtils.replace(origFile.getFilename(), 
                    (String)currentMetadata.get(MESSAGE_ID), newMessageId);
            copy(origFile, copyVm, newFilename);
        }
        
        return storeVM(copyVm);
    }
    
    public GridFSFile copy(GridFSDBFile origFile, DBObject destVm, String newFilename) {
        Assert.notNull(origFile);
        Assert.notNull(destVm);
        
        DBObject currentMetadata = origFile.getMetaData();
        
        BasicDBObject newMetadata = new BasicDBObject();
        newMetadata.putAll(currentMetadata);
        newMetadata.append(MongoConstants.ID, new ObjectId())
                   .append(VOICEMAIL_ID, (ObjectId)destVm.get(MongoConstants.ID));
        
        return m_gridFSTemplate.store(origFile.getInputStream(), newFilename
                , origFile.getContentType(), newMetadata);
    }
    
    public void move(User user, String messageId, String newLabel) {
        move(user, messageId, newLabel, false);
    }
    
    public void move(User user, String messageId, String newLabel, boolean markAsHeard) {
        Assert.notNull(user);

        DBObject results = findVM(user.getUserName(), messageId);
        if(results != null) {
            move(results, newLabel, markAsHeard);   
        }
    }
    
    public void move(String username, String messageId, String newLabel) {
        move(username, messageId, newLabel, false);
    }
    
    public void move(String username, String messageId, String newLabel, boolean markAsHeard) {
        DBObject results = findVM(username, messageId);
        if(results != null) {
            move(results, newLabel, markAsHeard);   
        }
    }    
    
    public DBObject findById(ObjectId id) {
        return doFindVM(new BasicDBObject(MongoConstants.ID, id), null);
    }
    
    public List<DBObject> findByLabel(User user, String label) {
        return findByLabel(user, label, false);
    }
    
    public List<DBObject> findByLabel(String username, String label) {
        return findByLabel(username, label, false);
    }
    
    public List<DBObject> findByLabel(User user, String label, boolean unheardOnly) {
        Assert.notNull(user);
        return findByLabel(user.getUserName(), label, unheardOnly);
    }
    
    public List<DBObject> findByLabel(String username, String label, boolean unheardOnly) {
        List<DBObject> results = findVMs(username, label, unheardOnly);
        return results != null && !results.isEmpty() ? results 
                                                    : new ArrayList<DBObject>();
    }
    
    public List<String> findMessageIdByLabel(User user, String label) {
        return findMessageIdByLabel(user, label, false);
    }
    
    public List<String> findMessageIdByLabel(String username, String label) {
        return findMessageIdByLabel(username, label, false);
    }
    
    public List<String> findMessageIdByLabel(User user, String label, boolean unheardOnly) {
        Assert.notNull(user);
        return findMessageIdByLabel(user.getUserName(), label, unheardOnly);
    }
    
    public List<String> findMessageIdByLabel(String username, String label, boolean unheardOnly) {
        List<DBObject> results = findVMs(username, label, unheardOnly
                , BasicDBObjectBuilder.start().add(MESSAGE_ID, 1)
                                              .add(TIMESTAMP, 1)
                                              .get()
                , BasicDBObjectBuilder.start().add(TIMESTAMP, -1)
                                              .get());
         
        List<String> messageIds = new ArrayList<>();
        if(results != null && !results.isEmpty()) {
            for(DBObject result :results) {
                messageIds.add((String)result.get(MESSAGE_ID));
            }
        }
        
        return messageIds;
    }
    
    public DBObject findByMessageId(User user, String messageId) {
        Assert.notNull(user);
        return findByMessageId(user.getUserName(), messageId);
    }
    
    public DBObject findByMessageId(String username, String messageId) {
        return findVM(username, messageId);
    }
    
    public DBObject findByMessageId(User user, String label, String messageId) {
        Assert.notNull(user);
        return findByMessageId(user.getUserName(), label, messageId);
    }
    
    public DBObject findByMessageId(String username, String label, String messageId) {
        return findVM(username, label, messageId);
    }
    
    public List<GridFSDBFile> findFilesByVmId(DBObject vmMetadata) {
        return findFilesByVmId((ObjectId)vmMetadata.get(MongoConstants.ID));
    }
    
    public List<GridFSDBFile> findFilesByVmId(ObjectId voicemailId) {
        return doFindFiles(voicemailId);
    }
    
    public GridFSDBFile findFileByAudioIdentifier(DBObject vmMetadata, VmAudioIdentifier ... audioPriorities) {
        if(vmMetadata != null && vmMetadata.containsField(MongoConstants.ID)) {
            return findFileByAudioIdentifier((ObjectId)vmMetadata.get(MongoConstants.ID), audioPriorities);
        }
        
        return null;
    }
    
    public GridFSDBFile findFileByAudioIdentifier(ObjectId voicemailId, VmAudioIdentifier ... audioPriorities) {
        List<GridFSDBFile> dbFiles = doFindFiles(voicemailId);
        if(dbFiles == null || dbFiles.isEmpty()) {
           return null; 
        }
        
        for(VmAudioIdentifier audioPriority : audioPriorities) {
            for(GridFSDBFile dbFile : dbFiles) {
                VmAudioIdentifier audioIdentifier = VmAudioIdentifier.lookUp(
                        (String)dbFile.getMetaData().get(GridFSVmTemplate.AUDIO_IDENTIFIER)
                    );
                if(audioPriority == audioIdentifier) {
                    return dbFile;
                }
            }
        }
        
        return null;
    }
    
    public GridFSDBFile findFileByFilename(DBObject vmMetadata, String filename) {
        if(vmMetadata != null && vmMetadata.containsField(MongoConstants.ID)) {
            return findFileByFilename((ObjectId)vmMetadata.get(MongoConstants.ID), filename);
        }
        
        return null;
    }
    
    public GridFSDBFile findFileByFilename(ObjectId voicemailId, String filename) {
        return doFindFile(voicemailId, filename);
    }
    
    public void delete(String username) {
        List<DBObject> vmMetadatas = doFindVMs(new BasicDBObject(GridFSVmTemplate.USER, username), null, null);
        for(DBObject vmMetadata : vmMetadatas) {
            delete(vmMetadata);
        }
    }
    
    public void delete(DBObject vmMetadata) {
        List<GridFSDBFile> dbFiles = findFilesByVmId(vmMetadata);
        delete(dbFiles); // Remove files first
        doRemoveVM(new BasicDBObject(MongoConstants.ID, vmMetadata.get(MongoConstants.ID)));
    }
    
    public void delete(List<GridFSDBFile> dbFiles) {
        for(GridFSDBFile dbFile : dbFiles) {
            delete(dbFile);
        }
    }
    
    public void delete(GridFSDBFile dbFile) {
        ObjectId id = (ObjectId)dbFile.get(MongoConstants.ID);
        m_gridFSTemplate.delete(new Query(
                    Criteria.where(MongoConstants.ID).is(id)
                ));
    }
    
    public void changeUser(User newUser, String oldUser) {
        DBCollection vmCollection = getVmCollection();
        vmCollection.update(new BasicDBObject(USER, oldUser)
                , BasicDBObjectBuilder.start().add(USER, newUser.getUserName())
                                              .add(USER_URI, newUser.getIdentity())
                                              .get());
    }
    
    private void createIndexes() {
        DBCollection filesCollection = getFilesCollection();
        DBCollection vmCollection = getVmCollection();

        filesCollection.createIndex(new BasicDBObject(METADATA_VOICEMAIL_ID, 1));

        // Create compound indexes
        vmCollection.createIndex(new BasicDBObject(USER, 1)
                                           .append(LABEL, 1)
                                           .append(MESSAGE_ID, 1));
    }

    private DBObject createFileMetadata(BasicDBObject metadata, ObjectId objectId
            , VmAudioIdentifier audioIdentifier, MessageDescriptor descriptor) {
        
        long timestamp = descriptor.getTimeStampDate() != null 
                ? descriptor.getTimeStampDate().getTime()
                : Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();

        return metadata.append(VOICEMAIL_ID, objectId)
                       .append(AUDIO_IDENTIFIER, audioIdentifier.toString())
                       .append(DURATION, descriptor.getDurationSecsLong())
                       .append(TIMESTAMP, timestamp)
                       .append(FILE_PATH, descriptor.getFilePath())
                       .append(AUDIO_FORMAT, descriptor.getAudioFormat())
                       .append(CONTENT_LENGTH, descriptor.getContentLength());
    }
    
    private DBObject createFileMetadata(BasicDBObject metadata, ObjectId objectId
            , VmAudioIdentifier audioIdentifier, TempMessage tempMessage) {
        
        return metadata.append(VOICEMAIL_ID, objectId)
                       .append(AUDIO_IDENTIFIER, audioIdentifier.toString())
                       .append(DURATION, tempMessage.getDuration())
                       .append(TIMESTAMP, tempMessage.getTimestamp())
                       .append(FILE_PATH, tempMessage.getTempPath())
                       .append(CONTENT_LENGTH, tempMessage.getContentLength());
    }
    
    public BasicDBObject createVmMetadata(DBObject metadata, VmAudioIdentifier audioIdentifier
            , String label, String messageId, TempMessage tempMessage
            , boolean unheard) {
        Assert.notNull(metadata);
        BasicDBObject basicMetadata = new BasicDBObject();
        basicMetadata.putAll(metadata);
        
        return createVmMetadata(basicMetadata, audioIdentifier, label, messageId, tempMessage, unheard);
    }
    
    public BasicDBObject createVmMetadata(BasicDBObject metadata, VmAudioIdentifier audioIdentifier
            , String label, String messageId, TempMessage tempMessage
            , boolean unheard) {
        Assert.notNull(metadata);
        Assert.notNull(tempMessage);
        
        // Set priority
        String priority = tempMessage.getPriority() != null 
                ? tempMessage.getPriority().getId() 
                : Priority.NORMAL.getId();

        metadata.append(USER, tempMessage.getCurrentUser())
                .append(LABEL, label)
                .append(MESSAGE_ID, messageId)
                .append(AUDIO_IDENTIFIER, audioIdentifier.toString())
                .append(UNHEARD, unheard)
                .append(FROM_URI, tempMessage.getFromUri())
                .append(TIMESTAMP, tempMessage.getTimestamp())
                .append(PRIORITY, priority);
        return metadata;
    }
    
    public BasicDBObject createVmMetadata(DBObject metadata, VmAudioIdentifier audioIdentifier
            , String label, String messageId, User destUser, MessageDescriptor descriptor
            , boolean unheard) {
        Assert.notNull(metadata);
        BasicDBObject basicMetadata = new BasicDBObject();
        basicMetadata.putAll(metadata);
        
        return createVmMetadata(basicMetadata, audioIdentifier, label, messageId, destUser
                , descriptor, unheard);
    }

    public BasicDBObject createVmMetadata(BasicDBObject metadata, VmAudioIdentifier audioIdentifier
            , String label, String messageId, User destUser, MessageDescriptor descriptor
            , boolean unheard) {
        Assert.notNull(metadata);
        Assert.notNull(destUser);

        metadata.append(USER, destUser.getUserName())
                .append(LABEL, label)
                .append(MESSAGE_ID, messageId)
                .append(AUDIO_IDENTIFIER, audioIdentifier.toString())
                .append(UNHEARD, unheard);
        return createVmMetadata(metadata, descriptor);
    }

    public BasicDBObject createVmMetadata(BasicDBObject metadata, MessageDescriptor descriptor) {
        Assert.notNull(metadata);
        Assert.notNull(descriptor);

        // Set priority
        String priority = descriptor.getPriority() != null 
                ? descriptor.getPriority().getId() 
                : Priority.NORMAL.getId();

        // Set Timestamp
        long timestamp = descriptor.getTimeStampDate() != null 
                ? descriptor.getTimeStampDate().getTime()
                : Calendar.getInstance(TimeZone.getTimeZone("GMT")).getTimeInMillis();

        // Build recipient list
        BasicDBList recipientList = new BasicDBList();
        
        List<String> descriptorRecipients = descriptor.getOtherRecipients();
        if (descriptorRecipients != null && !descriptorRecipients.isEmpty()) {
            for (String otherRecipient : descriptorRecipients) {
                recipientList.add(otherRecipient);
            }
        }

        // Fill up metadata
        metadata.append(USER_URI, descriptor.getId())
                .append(FROM_URI, descriptor.getFromUri())
                .append(SUBJECT, descriptor.getSubject())
                .append(TIMESTAMP, timestamp)
                .append(AUDIO_FORMAT, descriptor.getAudioFormat())
                .append(PRIORITY, priority).append(OTHER_RECIPIENTS, recipientList);

        return metadata;
    }
    
    public VmMessage createVmMessage(GridFSDBFile gridFSDBFile) throws IOException {
        DBObject fileMetadata = gridFSDBFile.getMetaData();
        DBObject vmMetadata = findById((ObjectId)fileMetadata.get(VOICEMAIL_ID));
        return createVmMessage(gridFSDBFile, vmMetadata, (TempMessage)null);
    }
    
    public VmMessage createVmMessage(GridFSDBFile gridFSDBFile, DBObject vmMetadata, TempMessage tempMessage) 
            throws IOException {
        File content = createLocalFile(gridFSDBFile, tempMessage);
        return createVmMessage(gridFSDBFile.getMetaData(), vmMetadata, content);
    }
    
    public VmMessage createVmMessage(DBObject fileMetadata, DBObject vmMetadata, File content) {
        MessageDescriptor messageDescriptor = createMessageDescriptor(fileMetadata, vmMetadata);
        Priority priority = Priority.valueOfById((String)vmMetadata.get(PRIORITY));
        Folder folder = Folder.tryLookUp((String)vmMetadata.get(LABEL));
        
        return new GridFSVmMessage((String)vmMetadata.get(MESSAGE_ID)
                           , (String)vmMetadata.get(USER), content
                           , messageDescriptor, folder
                           , (Boolean)vmMetadata.get(UNHEARD)
                           , priority == Priority.URGENT);    
    }
    
    public MessageDescriptor createMessageDescriptor(GridFSFile gridFSFile) {
        Assert.notNull(gridFSFile);
        DBObject fileMetadata = gridFSFile.getMetaData();
        DBObject vmMetadata = findById((ObjectId)fileMetadata.get(VOICEMAIL_ID));
        return createMessageDescriptor(gridFSFile.getMetaData(), vmMetadata);
    }

    public MessageDescriptor createMessageDescriptor(DBObject fileMetadata, DBObject vmMetadata) {
        MessageDescriptor descriptor = new MessageDescriptor();
        
        BasicDBObject basicVmMetadata = new BasicDBObject();
        basicVmMetadata.putAll(vmMetadata);
        descriptor.setId(basicVmMetadata.getString(USER_URI, StringUtils.EMPTY));
        descriptor.setFromUri(basicVmMetadata.getString(FROM_URI, StringUtils.EMPTY));
        descriptor.setSubject(basicVmMetadata.getString(SUBJECT, StringUtils.EMPTY));
        descriptor.setTimestamp(basicVmMetadata.getLong(TIMESTAMP, 0));
        descriptor.setPriority(Priority.valueOfById(basicVmMetadata.getString(PRIORITY, Priority.NORMAL.getId())));
        
        if (fileMetadata.containsField(OTHER_RECIPIENTS)) {
            BasicDBList otherRecipients = (BasicDBList) fileMetadata.get(OTHER_RECIPIENTS);
            for (Object recipient : otherRecipients) {
                descriptor.addOtherRecipient((String) recipient);
            }
        }

        BasicDBObject basicFileMetadata = new BasicDBObject();
        basicFileMetadata.putAll(fileMetadata);
        descriptor.setDurationSecs(basicFileMetadata.getLong(DURATION, 0));
        descriptor.setFilePath(basicFileMetadata.getString(FILE_PATH, StringUtils.EMPTY));
        descriptor.setAudioFormat(basicFileMetadata.getString(AUDIO_FORMAT, StringUtils.EMPTY));
        descriptor.setContentLength(basicFileMetadata.getString(CONTENT_LENGTH, StringUtils.EMPTY));
        
        return descriptor;
        
    }
    
    public File createLocalFile(GridFSDBFile dbFile, TempMessage tempMessage) 
            throws IOException {
        if(tempMessage != null) {
            File content = new File(tempMessage.getTempPath());
            dbFile.writeTo(content);
            return content;
        }
        
        return null;
    }
    
    protected List<DBObject> doFindVMs(DBObject query, DBObject keys, DBObject orderBy) {
        DBCollection collection = getVmCollection();

        DBCursor cursor = collection.find(query, keys);
        if(orderBy != null) {
            cursor.sort(orderBy);
        }
        
        List<DBObject> objects = new ArrayList<>();
        while (cursor.hasNext()) {
            objects.add(cursor.next());
        }

        return objects;
    }

    protected DBObject doFindVM(DBObject query, DBObject keys) {
        DBCollection collection = getVmCollection();
        return collection.findOne(query, keys);
    }
    
    protected void doRemoveVM(DBObject query) {
        DBCollection collection = getVmCollection();
        collection.remove(query);
    }
    
    protected GridFSDBFile doFindFile(ObjectId voicemailId, String filename) {
        Assert.notNull(voicemailId);
        return m_gridFSTemplate.findOne(new Query(
                    Criteria.where(METADATA_VOICEMAIL_ID).is(voicemailId)
                            .and(FILENAME).is(filename)
                ));
    }
    
    protected List<GridFSDBFile> doFindFiles(ObjectId voicemailId) {
        Assert.notNull(voicemailId);
        return m_gridFSTemplate.find(new Query(
                    Criteria.where(METADATA_VOICEMAIL_ID).is(voicemailId)
                ));
    }
    
    protected WriteResult doStoreVM(DBObject metadata) {
        DBCollection collection = getVmCollection();
        return collection.save(metadata);
    }
    
    private List<DBObject> findVMs(String username, String label, boolean unheardOnly) {
        return findVMs(username, label, false, null);
    }
    
    private List<DBObject> findVMs(String username, String label, boolean unheardOnly, DBObject fields) {
        return findVMs(username, label, unheardOnly, fields, null);
    }
    
    private List<DBObject> findVMs(String username, String label, boolean unheardOnly, DBObject fields, DBObject orderBy) {
        BasicDBObject query = new BasicDBObject(USER, username)
                                        .append(LABEL, label);
        if(unheardOnly) {
            query.append(UNHEARD, true);
        }
        return doFindVMs(query, fields, orderBy);
    }
    
    private DBObject findVM(String username, String messageId) {
        return findVM(username, messageId, (DBObject)null);
    }

    private DBObject findVM(String username, String messageId, DBObject fields) {
        BasicDBObject query = new BasicDBObject(USER, username)
                                        .append(MESSAGE_ID, messageId);
        return doFindVM(query, fields);
    }
    
    private DBObject findVM(String username, String label, String messageId) {
        return findVM(username, label, messageId, null);
    }

    private DBObject findVM(String username, String label, String messageId, DBObject fields) {
        BasicDBObject query = new BasicDBObject(USER, username)
                                        .append(LABEL, label)
                                        .append(MESSAGE_ID, messageId);
        return doFindVM(query, fields);
    }
    
    private void move(DBObject vmMetadata, String newLabel, boolean markAsHeard) {
        Assert.notNull(vmMetadata);
        
        if(!vmMetadata.containsField(MongoConstants.ID)) {
            vmMetadata = findByMessageId(
                    (String)vmMetadata.get(USER), 
                    (String)vmMetadata.get(LABEL),
                    (String)vmMetadata.get(MESSAGE_ID));
        }
        
        vmMetadata.put(LABEL, newLabel);
        if(markAsHeard) {
            vmMetadata.put(UNHEARD, false);
        }
        doStoreVM(vmMetadata);
    }
    
    private DBCollection getFilesCollection() {
        DB db = m_dbFactory.getDb();
        return db.getCollection(m_bucket + ".files");
    }

    private DBCollection getVmCollection() {
        DB db = m_dbFactory.getDb();
        return db.getCollection(m_bucket + ".metadata");
    }

    private static MongoConverter getDefaultMongoConverter(MongoDbFactory factory) {
        MappingMongoConverter converter = new MappingMongoConverter(factory, new MongoMappingContext());
        converter.afterPropertiesSet();
        return converter;
    }
}
