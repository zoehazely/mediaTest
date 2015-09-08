package org.sipfoundry.voicemail.mailbox.gridfs;

import org.sipfoundry.commons.mongo.MongoSequenceCounter;
import org.sipfoundry.commons.mongo.MongoSpringTemplate;

import com.mongodb.gridfs.GridFS;

public class GridFSSequenceCounter extends MongoSequenceCounter {

	private static final String MESSAGEID_COUNTER_SEQ_FORMAT = "%d%08d";
	
	private String m_ivrIdentityId;
	
	public GridFSSequenceCounter(MongoSpringTemplate dbTemplate, String ivrIdentityId) {
		this(dbTemplate, ivrIdentityId, GridFSVmTemplate.DEFAULT_BUCKET);
	}
	
	public GridFSSequenceCounter(MongoSpringTemplate dbTemplate, String ivrIdentityId, String bucket) {
		super(dbTemplate, bucket + ".counter");
		this.m_ivrIdentityId = ivrIdentityId;
	}
	
	public String getCurrentMessageId(String key) {
		long sequence = getCurrentSequence(key);
		return generateMessageId(sequence);
	}
	
	public String getNextMessageId(String key) {
		long sequence = getNextSequence(key);
		return generateMessageId(sequence);
	}
	
	private String generateMessageId(long sequenceNumber) {
		return String.format(MESSAGEID_COUNTER_SEQ_FORMAT
				, Integer.parseInt(m_ivrIdentityId)
				, sequenceNumber);
	}

	public String getIvrIdentityId() {
		return m_ivrIdentityId;
	}

	public void setIvrIdentityId(String ivrIdentityId) {
		this.m_ivrIdentityId = ivrIdentityId;
	}

}
