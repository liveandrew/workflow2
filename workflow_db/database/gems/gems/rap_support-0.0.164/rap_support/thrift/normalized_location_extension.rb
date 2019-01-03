module NormalizedLocationExtension
  def pretty_print
    [pretty_sub_region, pretty_region, pretty_country].compact.join(", ")
  end
end

class Rapleaf::Types::NewPersonData::NormalizedLocation
  include NormalizedLocationExtension
end
