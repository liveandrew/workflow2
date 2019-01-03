module Thrift
  module Struct

    def diff(other)
      d, total_diffs, struct_levels = self.differences(other, 0, 0)
      d[:total_diffs] = total_diffs
      d[:struct_levels] = struct_levels
      d
    end

    def differences(other, total_diffs, levels)
      levels += 1
      d = {}
      unless other.is_a?(self.class)
        d[:classes] = { :differ => true, :control => self.class, :extracted => other.class }
        total_diffs += 1
      else
        field_levels = 0
        each_field do |fid, field_info|
          name = field_info[:name]
          unless self.instance_variable_get("@#{name}") == other.instance_variable_get("@#{name}")
            if field_info[:type] == Thrift::Types::STRUCT && !self.instance_variable_get("@#{name}").nil? && !other.instance_variable_get("@#{name}").nil?
              d[name.intern], dfs, lvls = self.instance_variable_get("@#{name}").differences(other.instance_variable_get("@#{name}"), total_diffs, 0)
              total_diffs += dfs
              field_levels = lvls
            else
              d[name.intern] = {}
              d[name.intern][:control] = self.instance_variable_get("@#{name}")
              d[name.intern][:extracted] = other.instance_variable_get("@#{name}")
              total_diffs += 1
              field_levels += 1
            end
          end

        end
        levels += field_levels
      end
      [d, total_diffs, levels]
    end

  end

  class Union

    def diff(other)
      d, total_diffs, struct_levels = self.differences(other, 0, 0, fast_fail)
      d[:total_diffs] = total_diffs
      d[:struct_levels] = struct_levels
      d
    end

    def differences(other, total_diffs, levels)
      levels += 1
      d = {}
      unless other.is_a?(self.class)
        d[:classes] = { :differ => true, :control => self.class, :extracted => other.class }
        total_diffs += 1
      else
        unless self.get_value == other.get_value
          if self.get_value.class.ancestors.include?(Thrift::Struct) || other.get_value.class.ancestors.include?(Thrift::Union) && !self.get_value.nil? && !other.get_value.nil?
            d[self.get_set_field], dfs, lvls = self.get_value.differences(other.get_value, total_diffs, levels)
            total_diffs += dfs
            levels = lvls
          else
            d[self.get_set_field] = {}
            d[self.get_set_field][:control] = self.get_value
            d[self.get_set_field][:extracted] = other.get_value
            total_diffs += 1
            levels += 1
          end
        end
      end
      [d, total_diffs, levels]
    end

  end
end


class Differ
  def self.diff_du_arrays(left_arr, right_arr)
    left_arr.flatten!
    right_arr.flatten!
    raise Exception.new("Left Array contains elements which are not Data Units!") if left_arr.detect{|struct| !struct.class == Rapleaf::Types::NewPersonData::DataUnit }
    raise Exception.new("Right Array contains elements which are not Data Units!") if right_arr.detect{|struct| !struct.class == Rapleaf::Types::NewPersonData::DataUnit }

    ret_hash = {}
    left_groups = left_arr.group_by{ |du| du && du.data && du.data.get_set_field }
    right_groups = right_arr.group_by{ |du| du && du.data && du.data.get_set_field }

    # simple analysis
    ########################################

    # missing du types from one array or the other
    missing_left_keys = (left_groups.keys | right_groups.keys) - left_groups.keys
    missing_left_dus = missing_left_keys.inject({}){ |hsh, key| hsh[key] = right_groups[key]; hsh } unless missing_left_keys.empty?

    missing_right_keys = (left_groups.keys | right_groups.keys) - right_groups.keys
    missing_right_dus = missing_right_keys.inject({}){ |hsh, key| hsh[key] = left_groups[key]; hsh } unless missing_right_keys.empty?

    ret_hash[:missing_du_types] = {:extracted => missing_left_dus, :control => missing_right_dus} unless missing_left_keys.empty? && missing_right_keys.empty?
    # we know the differences for these dus, delete them so we don't skew the other results or do unnecessary work
    missing_left_keys.each{ |key| right_groups.delete(key) }
    missing_right_keys.each{ |key| left_groups.delete(key) }



    # more of a du type in one array than the other.
    keys = left_groups.keys # should be identical to right group keys now
    mismatched_du_counts = keys.inject({}) do |hsh,key|
      hsh[key] = {:control => left_groups[key].size, :extracted => right_groups[key].size} unless left_groups[key].size == right_groups[key].size
      hsh
    end

    ret_hash[:mismatched_du_count] = mismatched_du_counts unless mismatched_du_counts.empty?


    # deep analysis
    ########################################

    # reduce problem space
    # use sets to get dus within a group that don't have a corresponding du in the other group
    different_dus_by_type = {}
    keys.each do |key|
      next if left_groups[key] == right_groups[key]
      exclusively_left = left_groups[key] - right_groups[key]
      exclusively_right = right_groups[key] - left_groups[key]
      
      different_dus_by_type[key] = {:control => exclusively_left, :extracted => exclusively_right}
    end


    # deep diffing
    du_diffs = {}
    different_dus_by_type.each do |pairs|
      key, dus = pairs

      if dus[:control].empty? && dus[:extracted].empty?
        # hsh[key] = :match
        next
      elsif dus[:control].empty? || dus[:extracted].empty?
        du_diffs[key] = {:control => dus[:control], :extracted => dus[:extracted] }
        next
      end

      diffs = []
      matches = []
      scored = []
      finished = []

      dus[:control].each do |l|
        dus[:extracted].each do |r|
          d = l.diff(r)

          scored << { :control => l, :extracted => r, :score => d[:total_diffs].to_f / d[:struct_levels].to_f, :diffs => d}
        end
      end

      sorted_scored = scored.sort_by{|s| s[:score]  }

      min = sorted_scored.first[:score]
      while !sorted_scored.empty? && min < 0.75

        best = sorted_scored.shift
        next if finished.include?(best[:control]) || finished.include?(best[:extracted])

        unless best[:diffs][:total_diffs] == 0
          diffs << {:control => best[:control], :extracted => best[:extracted], :diffs => best[:diffs]}
        else
          matches << {:control => best[:control], :extracted => best[:extracted]}
        end

        # sorted_scored.delete_if { |s| s[:control] == best[:control] || s[:extracted] == best[:control] || s[:control] == best[:extracted] || s[:extracted] == best[:extracted]  }
        finished << best[:control]
        finished << best[:extracted]

        min = sorted_scored.first[:score] unless sorted_scored.empty?
      end

      left_remaining = (dus[:control] - finished)
      right_remaining = (dus[:extracted] - finished)
      unless (left_remaining | right_remaining).empty?
        diffs << {:control => left_remaining, :extracted => right_remaining, :diffs => :no_good_diffs }
      end

      du_diffs[key] = diffs
    end
    ret_hash[:du_diffs] = du_diffs unless du_diffs.empty?

    ret_hash
  end
end
