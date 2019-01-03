module PinExtension

  def es_userid?
    es_pin? && es_pin.identifier.userid?
  end

  def es_username?
    es_pin? && es_pin.identifier.username?
  end

  def format
    if email?
      "#{email}"
    elsif entity_id?
      "Entity ID::#{entity_id.unpack('H*').first}"
    elsif es_pin?
      "#{Rapleaf::Types::PersonData::EsSite::VALUE_MAP[es_pin.site].downcase}::#{es_pin.identifier.get_value}"
    elsif url?
      "#{url}"
    elsif hashed_email?
      if hashed_email.sha1?
        "sha1::#{hashed_email.sha1.to_hex}"
      elsif hashed_email.md5?
        "md5::#{hashed_email.md5.to_hex}"
      else
        "Unformatted hashed email PIN: #{CGI::escapeHTML(inspect)}"
      end
    elsif napkin?
      postal_string = ""
      postal_string << napkin.first_name.titleize if napkin.first_name != nil
      postal_string << " " + napkin.last_name.titleize if napkin.last_name != nil
      postal_string << ",  " + napkin.street.titleize if napkin.street != nil
      postal_string << ",  " + napkin.city.titleize if napkin.city != nil
      postal_string << ",  " + napkin.state if napkin.state != nil
      postal_string << "  " + napkin.zip if napkin.zip != nil
      postal_string
    elsif data_partner_pin?
      "partner::#{Rapleaf::Types::PersonData::DataPartner::VALUE_MAP[data_partner_pin.partner].downcase}::#{data_partner_pin.id}"
    elsif group?
      if group.zip?
        "zip::#{group.zip}"
      elsif group.zip_plus_4?
        "zip_plus_4::#{group.zip_plus_4}"
      elsif group.zip_plus_6?
        "zip_plus_6::#{group.zip_plus_6}"
      elsif group.name_and_city?
        name_and_city_string = "name_and_city::"
        name_and_city_string << group.name_and_city.first_name.titleize if group.name_and_city.first_name != nil
        name_and_city_string << " " + group.name_and_city.last_name.titleize if group.name_and_city.last_name != nil
        name_and_city_string << ",  " + group.name_and_city.city.titleize if group.name_and_city.city != nil
        name_and_city_string << ",  " + group.name_and_city.state if group.name_and_city.state != nil
        name_and_city_string
      else
        "Unformatted group PIN: #{CGI::escapeHTML(inspect)}"
      end
    else
      "Unformatted domain: #{CGI::escapeHTML(inspect)}"
    end

  end

  def format_long
    if email?
      "email::#{email}"
    elsif entity_id?
      "entity_id::#{entity_id.unpack('H*').first}"
    elsif es_pin?
      "#{es_pin.identifier.get_set_field}::#{Rapleaf::Types::PersonData::EsSite::VALUE_MAP[es_pin.site].downcase}::#{es_pin.identifier.get_value}"
    elsif url?
      "url::#{url}"
    elsif hashed_email?
      if hashed_email.sha1?
        "hash::sha1::#{hashed_email.sha1.to_hex}"
      elsif hashed_email.md5?
        "hash::md5::#{hashed_email.md5.to_hex}"
      else
        "Unformatted hashed email PIN: #{CGI::escapeHTML(inspect)}"
      end
    elsif napkin?
      "NAP::#{napkin.inspect.match(/<Rapleaf::Types::NewPersonData::NAPkin (.+)/)[1].chomp(">").gsub(/"/, "")}"
    elsif data_partner_pin?
      "partner::#{Rapleaf::Types::PersonData::DataPartner::VALUE_MAP[data_partner_pin.partner].downcase}::#{data_partner_pin.id}"
    elsif group?
      if group.zip?
        "group::zip::#{group.zip}"
      elsif group.zip_plus_4?
        "group::zip_plus_4::#{group.zip_plus_4}"
      elsif group.zip_plus_6?
        "group::zip_plus_4::#{group.zip_plus_6}"
      elsif group.name_and_city?
        "group::name_and_city::#{group.name_and_city.inspect.match(/<Rapleaf::Types::NewPersonData::NameAndCity (.+)/)[1].chomp(">").gsub(/"/, "")}"
      else
        "Unformatted group PIN: #{CGI::escapeHTML(inspect)}"
      end
    else
      "Unformatted PIN: #{CGI::escapeHTML(inspect)}"
    end
  end

  # parses the output of format_long back into a pin
  def parse(str)
    type, value = str.split('::', 2)
    pin = self
    case type
    when 'email'
      pin.email = value
    when 'entity_id'
      pin.entity_id = [value].pack('H*')
    when 'userid', 'username'
      site, id = value.split('::', 2)
      site = eval("Rapleaf::Types::PersonData::EsSite::#{site.upcase}")
      identifier = Rapleaf::Types::NewPersonData::UserIdOrName.new
      identifier.send("#{type}=", id)
      pin.es_pin = Rapleaf::Types::NewPersonData::EsPIN.new(:site => site, :identifier => identifier)
    when 'url'
      pin.url = value
    when 'hash'
      type, hex = value.split('::', 2)
      pin.hashed_email = Rapleaf::Types::NewPersonData::HashedEmailPIN.new
      pin.hashed_email.send("#{type}=", hex.to_bytes)
    when 'NAP'
      pin.napkin = Rapleaf::Types::NewPersonData::NAPkin.new
      pin.napkin.first_name = value.match(/first_name:(.+?)(,|$)/).nil? ? nil : $1
      pin.napkin.last_name = value.match(/last_name:(.+?)(,|$)/).nil? ? nil : $1
      pin.napkin.street = value.match(/street:(.+?)(,|$)/).nil? ? nil : $1
      pin.napkin.city = value.match(/city:(.+?)(,|$)/).nil? ? nil : $1
      pin.napkin.state = value.match(/state:(.+?)(,|$)/).nil? ? nil : $1
      pin.napkin.zip = value.match(/zip:(.+?)(,|$)/).nil? ? nil : $1
    when 'partner'
      partner, id = value.split('::', 2)
      partner = eval("Rapleaf::Types::PersonData::DataPartner::#{partner.upcase}")
      dp = Rapleaf::Types::NewPersonData::DataPartnerPIN.new
      dp.partner = partner
      dp.id = id
      pin.data_partner_pin = dp
    when 'group'
      type, group_string = value.split('::', 2)
      pin.group = Rapleaf::Types::NewPersonData::GroupPIN.new
      if type == 'name_and_city'
        pin.group.name_and_city = Rapleaf::Types::NewPersonData::NameAndCity.new
        pin.group.name_and_city.first_name = group_string.match(/first_name:(.+?)(,|$)/).nil? ? nil : $1
        pin.group.name_and_city.last_name = group_string.match(/last_name:(.+?)(,|$)/).nil? ? nil : $1
        pin.group.name_and_city.city = group_string.match(/city:(.+?)(,|$)/).nil? ? nil : $1
        pin.group.name_and_city.state = group_string.match(/state:(.+?)(,|$)/).nil? ? nil : $1
      else
        pin.group.send("#{type}=", group_string)
      end
    when 'username'
      site_string, username_string = value.split('::', 2)
      site = "Rapleaf::Types::PersonData::EsSite::#{site_string.upcase}".constantize
      pin.es_pin = EsPIN.new(:site => site, :identifier => UserIdOrName.new(:username => username_string))
    when 'userid'
      site_string, userid_string = value.split('::', 2)
      site = "Rapleaf::Types::PersonData::EsSite::#{site_string.upcase}".constantize
      pin.es_pin = EsPIN.new(:site => site, :identifier => UserIdOrName.new(:userid => userid_string))
    else
      raise "Don't know how to parse PIN #{str}."
    end
    self
  end

  def <=>(other)
    "#{get_set_field}#{format}" <=> "#{other.get_set_field}#{other.format}"
  end
end

class Rapleaf::Types::NewPersonData::PIN
  include PinExtension
end
