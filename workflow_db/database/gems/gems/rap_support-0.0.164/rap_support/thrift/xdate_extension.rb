module XDateExtension
  def to_s
    # If value is not set, you'll get zeros
    sprintf("%04d-%02d-%02d", year.to_i, month.to_i, day.to_i)
  end
  
  def to_date
    Time.local(year.to_i, month.to_i, day.to_i).to_date
  end

  # stupid hack required to allow class methods to be mixed in with modules.
  def self.included(klass)
    klass.extend(ClassMethods)
  end

  module ClassMethods
    def from_date(date, include_fields = [:year, :month, :day])
      Rapleaf::Types::NewPersonData::XDate.new(include_fields.inject({}) {|h, field| h[field] = date.send(field); h})
    end
  end

end

class Rapleaf::Types::NewPersonData::XDate
  include XDateExtension
end
