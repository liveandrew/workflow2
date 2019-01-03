module EsPINExtension
  # Returns UPPERCASE name
  def site_name
    Rapleaf::Types::PersonData::EsSite::VALUE_MAP[site] || "UNKNOWN"
  end

  def userid_or_username
    identifier.get_value
  end
end

class Rapleaf::Types::NewPersonData::EsPIN
  include EsPINExtension
end
