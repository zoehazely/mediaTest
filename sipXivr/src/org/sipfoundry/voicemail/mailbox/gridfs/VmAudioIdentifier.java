package org.sipfoundry.voicemail.mailbox.gridfs;

public enum VmAudioIdentifier {
    ORIGINAL("-01.%s"),
    CURRENT("-00.%s"),
    COMBINED("-FW.%s");
    
    private final String m_format;
    
    private VmAudioIdentifier(String format) {
        this.m_format = format;
    }
    
    public String getFormat() {
        return m_format;
    }
    
    public static VmAudioIdentifier lookUp(String name) {
        VmAudioIdentifier identifier = tryLookUp(name);
        if(identifier != null) {
            return identifier;
        }
        
        throw new IllegalArgumentException("Invalid Vm Audio Identifier");
    }
    
    public static VmAudioIdentifier tryLookUp(String name) {
        for(VmAudioIdentifier identifier : values()) {
            if(identifier.toString().equalsIgnoreCase(name)) {
                return identifier;
            }
        }
        
        return null;
    }
}
