/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.ivyft.kafka.yarn.protocol;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class NodeContainer extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"NodeContainer\",\"namespace\":\"com.ivyft.kafka.yarn.protocol\",\"fields\":[{\"name\":\"nodeHost\",\"type\":\"string\",\"doc\":\"Container 运行的 Host\"},{\"name\":\"nodePort\",\"type\":\"int\",\"doc\":\"Container RPC port\"},{\"name\":\"containerId\",\"type\":\"string\",\"doc\":\"Yarn 分配的 Container Id\"},{\"name\":\"nodeHttpAddress\",\"type\":\"string\",\"doc\":\"Kafka ApplicationMaster Tracker URI\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  /** Container 运行的 Host */
  @Deprecated public java.lang.CharSequence nodeHost;
  /** Container RPC port */
  @Deprecated public int nodePort;
  /** Yarn 分配的 Container Id */
  @Deprecated public java.lang.CharSequence containerId;
  /** Kafka ApplicationMaster Tracker URI */
  @Deprecated public java.lang.CharSequence nodeHttpAddress;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public NodeContainer() {}

  /**
   * All-args constructor.
   */
  public NodeContainer(java.lang.CharSequence nodeHost, java.lang.Integer nodePort, java.lang.CharSequence containerId, java.lang.CharSequence nodeHttpAddress) {
    this.nodeHost = nodeHost;
    this.nodePort = nodePort;
    this.containerId = containerId;
    this.nodeHttpAddress = nodeHttpAddress;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return nodeHost;
    case 1: return nodePort;
    case 2: return containerId;
    case 3: return nodeHttpAddress;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: nodeHost = (java.lang.CharSequence)value$; break;
    case 1: nodePort = (java.lang.Integer)value$; break;
    case 2: containerId = (java.lang.CharSequence)value$; break;
    case 3: nodeHttpAddress = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'nodeHost' field.
   * Container 运行的 Host   */
  public java.lang.CharSequence getNodeHost() {
    return nodeHost;
  }

  /**
   * Sets the value of the 'nodeHost' field.
   * Container 运行的 Host   * @param value the value to set.
   */
  public void setNodeHost(java.lang.CharSequence value) {
    this.nodeHost = value;
  }

  /**
   * Gets the value of the 'nodePort' field.
   * Container RPC port   */
  public java.lang.Integer getNodePort() {
    return nodePort;
  }

  /**
   * Sets the value of the 'nodePort' field.
   * Container RPC port   * @param value the value to set.
   */
  public void setNodePort(java.lang.Integer value) {
    this.nodePort = value;
  }

  /**
   * Gets the value of the 'containerId' field.
   * Yarn 分配的 Container Id   */
  public java.lang.CharSequence getContainerId() {
    return containerId;
  }

  /**
   * Sets the value of the 'containerId' field.
   * Yarn 分配的 Container Id   * @param value the value to set.
   */
  public void setContainerId(java.lang.CharSequence value) {
    this.containerId = value;
  }

  /**
   * Gets the value of the 'nodeHttpAddress' field.
   * Kafka ApplicationMaster Tracker URI   */
  public java.lang.CharSequence getNodeHttpAddress() {
    return nodeHttpAddress;
  }

  /**
   * Sets the value of the 'nodeHttpAddress' field.
   * Kafka ApplicationMaster Tracker URI   * @param value the value to set.
   */
  public void setNodeHttpAddress(java.lang.CharSequence value) {
    this.nodeHttpAddress = value;
  }

  /** Creates a new NodeContainer RecordBuilder */
  public static com.ivyft.kafka.yarn.protocol.NodeContainer.Builder newBuilder() {
    return new com.ivyft.kafka.yarn.protocol.NodeContainer.Builder();
  }
  
  /** Creates a new NodeContainer RecordBuilder by copying an existing Builder */
  public static com.ivyft.kafka.yarn.protocol.NodeContainer.Builder newBuilder(com.ivyft.kafka.yarn.protocol.NodeContainer.Builder other) {
    return new com.ivyft.kafka.yarn.protocol.NodeContainer.Builder(other);
  }
  
  /** Creates a new NodeContainer RecordBuilder by copying an existing NodeContainer instance */
  public static com.ivyft.kafka.yarn.protocol.NodeContainer.Builder newBuilder(com.ivyft.kafka.yarn.protocol.NodeContainer other) {
    return new com.ivyft.kafka.yarn.protocol.NodeContainer.Builder(other);
  }
  
  /**
   * RecordBuilder for NodeContainer instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<NodeContainer>
    implements org.apache.avro.data.RecordBuilder<NodeContainer> {

    private java.lang.CharSequence nodeHost;
    private int nodePort;
    private java.lang.CharSequence containerId;
    private java.lang.CharSequence nodeHttpAddress;

    /** Creates a new Builder */
    private Builder() {
      super(com.ivyft.kafka.yarn.protocol.NodeContainer.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(com.ivyft.kafka.yarn.protocol.NodeContainer.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.nodeHost)) {
        this.nodeHost = data().deepCopy(fields()[0].schema(), other.nodeHost);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.nodePort)) {
        this.nodePort = data().deepCopy(fields()[1].schema(), other.nodePort);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.containerId)) {
        this.containerId = data().deepCopy(fields()[2].schema(), other.containerId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.nodeHttpAddress)) {
        this.nodeHttpAddress = data().deepCopy(fields()[3].schema(), other.nodeHttpAddress);
        fieldSetFlags()[3] = true;
      }
    }
    
    /** Creates a Builder by copying an existing NodeContainer instance */
    private Builder(com.ivyft.kafka.yarn.protocol.NodeContainer other) {
            super(com.ivyft.kafka.yarn.protocol.NodeContainer.SCHEMA$);
      if (isValidValue(fields()[0], other.nodeHost)) {
        this.nodeHost = data().deepCopy(fields()[0].schema(), other.nodeHost);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.nodePort)) {
        this.nodePort = data().deepCopy(fields()[1].schema(), other.nodePort);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.containerId)) {
        this.containerId = data().deepCopy(fields()[2].schema(), other.containerId);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.nodeHttpAddress)) {
        this.nodeHttpAddress = data().deepCopy(fields()[3].schema(), other.nodeHttpAddress);
        fieldSetFlags()[3] = true;
      }
    }

    /** Gets the value of the 'nodeHost' field */
    public java.lang.CharSequence getNodeHost() {
      return nodeHost;
    }
    
    /** Sets the value of the 'nodeHost' field */
    public com.ivyft.kafka.yarn.protocol.NodeContainer.Builder setNodeHost(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.nodeHost = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'nodeHost' field has been set */
    public boolean hasNodeHost() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'nodeHost' field */
    public com.ivyft.kafka.yarn.protocol.NodeContainer.Builder clearNodeHost() {
      nodeHost = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'nodePort' field */
    public java.lang.Integer getNodePort() {
      return nodePort;
    }
    
    /** Sets the value of the 'nodePort' field */
    public com.ivyft.kafka.yarn.protocol.NodeContainer.Builder setNodePort(int value) {
      validate(fields()[1], value);
      this.nodePort = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'nodePort' field has been set */
    public boolean hasNodePort() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'nodePort' field */
    public com.ivyft.kafka.yarn.protocol.NodeContainer.Builder clearNodePort() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /** Gets the value of the 'containerId' field */
    public java.lang.CharSequence getContainerId() {
      return containerId;
    }
    
    /** Sets the value of the 'containerId' field */
    public com.ivyft.kafka.yarn.protocol.NodeContainer.Builder setContainerId(java.lang.CharSequence value) {
      validate(fields()[2], value);
      this.containerId = value;
      fieldSetFlags()[2] = true;
      return this; 
    }
    
    /** Checks whether the 'containerId' field has been set */
    public boolean hasContainerId() {
      return fieldSetFlags()[2];
    }
    
    /** Clears the value of the 'containerId' field */
    public com.ivyft.kafka.yarn.protocol.NodeContainer.Builder clearContainerId() {
      containerId = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /** Gets the value of the 'nodeHttpAddress' field */
    public java.lang.CharSequence getNodeHttpAddress() {
      return nodeHttpAddress;
    }
    
    /** Sets the value of the 'nodeHttpAddress' field */
    public com.ivyft.kafka.yarn.protocol.NodeContainer.Builder setNodeHttpAddress(java.lang.CharSequence value) {
      validate(fields()[3], value);
      this.nodeHttpAddress = value;
      fieldSetFlags()[3] = true;
      return this; 
    }
    
    /** Checks whether the 'nodeHttpAddress' field has been set */
    public boolean hasNodeHttpAddress() {
      return fieldSetFlags()[3];
    }
    
    /** Clears the value of the 'nodeHttpAddress' field */
    public com.ivyft.kafka.yarn.protocol.NodeContainer.Builder clearNodeHttpAddress() {
      nodeHttpAddress = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    @Override
    public NodeContainer build() {
      try {
        NodeContainer record = new NodeContainer();
        record.nodeHost = fieldSetFlags()[0] ? this.nodeHost : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.nodePort = fieldSetFlags()[1] ? this.nodePort : (java.lang.Integer) defaultValue(fields()[1]);
        record.containerId = fieldSetFlags()[2] ? this.containerId : (java.lang.CharSequence) defaultValue(fields()[2]);
        record.nodeHttpAddress = fieldSetFlags()[3] ? this.nodeHttpAddress : (java.lang.CharSequence) defaultValue(fields()[3]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}