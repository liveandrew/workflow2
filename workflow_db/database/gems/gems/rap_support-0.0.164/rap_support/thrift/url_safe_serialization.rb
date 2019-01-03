module UrlSafeSerialization

  def self.serialize_url_safe(object)
    @@serializer ||= Thrift::Serializer.new(Thrift::CompactProtocolFactory.new)
    data = @@serializer.serialize(object)
    data = Base64.encode64(data)
    data = data.chomp #encode64 adds a wild newline
    data = CGI.escape(data)
    data
  end

  def self.deserialize_url_safe(object, data)
    raise "Data to be deserialized is nil." if data.nil?
    data = Base64.decode64(data)
    @@deserializer ||= Thrift::Deserializer.new(Thrift::CompactProtocolFactory.new)
    data = @@deserializer.deserialize(object, data)
    data
  end

end

module Thrift
  module Struct
    def serialize_url_safe
      UrlSafeSerialization.serialize_url_safe(self)
    end
    def deserialize_url_safe(data)
      UrlSafeSerialization.deserialize_url_safe(self, data)
    end
  end

  class Union
    def serialize_url_safe
      UrlSafeSerialization.serialize_url_safe(self)
    end
    def deserialize_url_safe(data)
      UrlSafeSerialization.deserialize_url_safe(self, data)
    end
  end
end
