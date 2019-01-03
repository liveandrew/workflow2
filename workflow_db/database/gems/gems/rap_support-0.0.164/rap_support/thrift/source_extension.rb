module SourceExtension
  # Returns UPPERCASE name
  def site_name
    if es_site
      Rapleaf::Types::PersonData::EsSite::VALUE_MAP[es_site] || "UNKNOWN"
    else
      nil
    end
  end
end

class Rapleaf::Types::NewPersonData::Source
  include SourceExtension
end
