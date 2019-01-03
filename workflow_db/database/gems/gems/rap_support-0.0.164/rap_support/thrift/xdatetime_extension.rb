module XDateTimeExtension
  def to_s
    # If value is not set, you'll get zeros
    sprintf("%04d-%02d-%02d", year.to_i, month.to_i, day.to_i)
  end
  
  def to_datetime
    Time.local(year.to_i, month.to_i, day.to_i, hour.to_i, minute.to_i).to_datetime
  end

  # stupid hack required to allow class methods to be mixed in with modules.
  def self.included(klass)
    klass.extend(ClassMethods)
  end

  module ClassMethods
    def from_datetime(datetime, include_fields = [:year, :month, :day, :hour, :minute])
      Rapleaf::Types::NewPersonData::XDateTime.new(include_fields.inject({}) {|h, field| h[field] = datetime.send(field.to_s.gsub(/^minute$/, 'min')); h})
    end
  end
  
end

class Rapleaf::Types::NewPersonData::XDateTime
  include XDateTimeExtension
end

