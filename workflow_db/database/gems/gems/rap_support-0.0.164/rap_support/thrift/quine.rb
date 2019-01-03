

# Adds quine methods to cause objects to print themselves out as eval-able strings.

class Object
  def quine
    self.inspect
  end
end

class Array
  def quine
    qs = self.map{|e| e.quine}
    q = qs.reject{|e| e.nil?}.join(', ')
    "[#{q}]"
  end
end

class Hash
  def quine
    q = self.map do |k,v|
      kq = k.quine
      vq = v.quine
      if kq && !kq.empty? && vq && !vq.empty?
        "#{kq} => #{vq}"
      else
        nil
      end
    end

    qs = q.reject{|e| e.nil?}.join(', ')
    "{#{qs}}"
  end
end

class EnumConst__ #wrapper to handle enum constants differently
  def initialize(v); @value = v; end
  def quine; @value; end
end

module Thrift
  module Struct
    def quine
      type_name = self.class.name =~ /PersonData::(\w+)/ && $1
      struct_type = if Rapleaf::Types::NewPersonData.const_defined?(type_name)
                      Rapleaf::Types::NewPersonData.const_get(type_name)
                    elsif Rapleaf::Types::PersonData.const_defined?(type_name)
                      Rapleaf::Types::PersonData.const_get(type_name)
                    else
                      throw Exception.new("Not a Rapleaf::Type! #{thrift_struct.inspect}")
                    end

      data = struct_fields.map do |_,v| 
        d = send(v[:name])
        d = EnumConst__.new("#{struct_fields[_][:enum_class].to_s.split('::').last}::#{struct_fields[_][:enum_class]::VALUE_MAP[d]}") if struct_fields[_][:enum_class] && d
        d ? {v[:name] => d} : nil
      end
      
      data = data.select { |v| v }
      
      quines = data.map do |e| 
        e.map do |k,v|
          q = v.quine
          q.empty? ? nil : {k => q}
        end.compact
      end
      
      #puts quines.inspect
      quines.flatten!
      
      contents = quines.map do |e| 
        e.map do |k,v| 
          ":#{k} => #{v}"
        end
      end.flatten.join(', ')
      
      contents = "#{contents}" if contents && contents.size > 0

      "#{struct_type.to_s.split('::').last}.new(#{contents})"
    end
  end

  class Union
    def quine
      type_name = self.class.name =~ /PersonData::(\w+)/ && $1
      struct_type = if Rapleaf::Types::NewPersonData.const_defined?(type_name)
                      Rapleaf::Types::NewPersonData.const_get(type_name)
                    elsif Rapleaf::Types::PersonData.const_defined?(type_name)
                      Rapleaf::Types::PersonData.const_get(type_name)
                    else
                      throw Exception.new("Not a Rapleaf::Type! #{thrift_struct.inspect}")
                    end
      
      if (_enum_class = struct_fields[name_to_id(get_set_field.to_s)][:enum_class])
        q = "#{_enum_class.to_s.split('::').last}::#{_enum_class::VALUE_MAP[get_value] || get_value}"
      else
        q = "#{get_value.quine}"
      end
      "#{struct_type.to_s.split('::').last}.#{get_set_field}(#{q})"
    end    
  end
end


