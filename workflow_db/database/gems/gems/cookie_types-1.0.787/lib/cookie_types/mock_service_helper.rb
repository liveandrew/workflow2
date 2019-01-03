require 'thrift'

class MockServiceHelper
  def self.load_model_from_json(model_class)
    des = Thrift::Deserializer.new(Thrift::JsonProtocolFactory.new)
    path = File.dirname(__FILE__)
    des.deserialize(model_class.new, File.read("#{path}/../fixtures/#{class_to_file_name(model_class)}.json"))
  end

  def self.convert_model_to_json(object_instance)
    ser = Thrift::Serializer.new(Thrift::JsonProtocolFactory.new)
    ser.serialize(object_instance)
  end

  private

  def self.class_to_file_name(klass)
    klass.name.split('::').map(&:underscore).join('/')
  end
end
