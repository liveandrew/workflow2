module Thrift
  module Struct

    def pretty_print(q)
      q.group(2, "#<#{self.class.name.split('::').last} ", " >") do
        thrift_pp_sub(q, true)
      end
    end

    def thrift_pp_sub(q, first)
      fields = []
      each_field{|_,field| fields << field unless instance_variable_get("@#{field[:name]}").nil?}
      q.seplist(fields) do |field_info|
        q.group(2) do
          q.text field_info[:name].intern.inspect
          q.text ' => '
          q.group(2, '', '') do 
            if self.instance_variable_get("@#{field_info[:name]}").respond_to?(:thrift_pp_sub)
              self.instance_variable_get("@#{field_info[:name]}").thrift_pp_sub(q, false)
            else
              if field_info[:enum_class]
                __value = self.instance_variable_get("@#{field_info[:name]}")
                q.text "#{field_info[:enum_class].to_s.split('::').last}::#{field_info[:enum_class]::VALUE_MAP[__value] || __value}"
              else
                q.pp self.instance_variable_get("@#{field_info[:name]}")
              end
            end
          end
        end
      end
    end

    def pretty_print_cycle(q)
      q.text sprintf("<%s:...>", self.class.name.split('::').last)
    end

  end


  class Union
    def pretty_print(q)
      q.group(2, "#<#{self.class.name.split('::').last} ", " >") do
        thrift_pp_sub(q, true)
      end
    end

    def thrift_pp_sub(q, first)
      if get_set_field
        q.text get_set_field.inspect
        q.text ' => '
        q.group(2, '', '') do 
          if get_value.respond_to?(:thrift_pp_sub)
            get_value.thrift_pp_sub(q, false)
          else
            if (_enum_class = struct_fields[name_to_id(get_set_field.to_s)][:enum_class])
              q.text "#{_enum_class.to_s.split('::').last}::#{_enum_class::VALUE_MAP[get_value] || get_value}"
            else
              q.pp get_value
            end
          end
        end
      else
        q.text 'nil'
      end
    end

    def pretty_print_cycle(q)
      q.text sprintf("<%s:...>", self.class.name.split('::').last )
    end

  end

  
end
