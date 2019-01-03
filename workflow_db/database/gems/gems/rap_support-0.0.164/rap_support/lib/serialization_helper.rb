class SerializationHelper
  def SerializationHelper.inflate(value)
    return unless value
    io      = StringIO.new(value)
    gzr     = Zlib::GzipReader.new(io)
    data = ""
    gzr.each { |b| data << b }
    io.close
    data
  end

  def SerializationHelper.serialize(data, protocol=Thrift::CompactProtocolFactory.new)
    @@serializer ||= Thrift::Serializer.new(protocol)
    data ? @@serializer.serialize(data) : data
  end

  def SerializationHelper.deserialize(obj, data, protocol=Thrift::CompactProtocolFactory.new)
    return unless data
    @@deserializer ||= Thrift::Deserializer.new(protocol)
    data ? @@deserializer.deserialize(obj, data) : data
  end
end
