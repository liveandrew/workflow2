require 'cookie_helper'

module LiverampCookieHelper

  def self.decrypt_liveramp_cookie(cookie_str)
    return CookieHelper.decrypt_cookie_base64(cookie_str, cookie = Rapleaf::Types::Spruce::LiveRampCookie.new)
  end

  def self.get_liveramp_cookie_header(cookie_id_hex, heid_hex)
    base_cookie = get_liveramp_cookie(cookie_id_hex, heid_hex)
    return {'Cookie' => "rlas3=\"#{CookieHelper.get_ciphertext_and_iv_base64(SerializationHelper.serialize(base_cookie))[0].strip}\";"}
  end

  def self.get_liveramp_cookie(cookie_id_hex, heid_hex)
    cookie = Rapleaf::Types::Spruce::LiveRampCookie.new
    
    cookie.first_cookied_at = 0
    cookie.last_cookied_at = 0
    cookie.cookie_id = [cookie_id_hex].pack("H*")
    cookie.heid = [heid_hex].pack("H*")
    
    matched_heid = Rapleaf::Types::Spruce::MatchedHeid.new
    matched_heid.heid = cookie.heid
    matched_heid.liveramp_source = Rapleaf::Types::Spruce::LiveRampDeviceIdSource::IDENTIFIER_MATCHING
    
    cookie.heid_by_subnetworks = {8 => matched_heid}
    cookie.super_cookie_cleaned = true

    match = Rapleaf::Types::Spruce::LastPublisherMatch.new
    match.publisher_id = 1000
    match.timestamp = Time.now.to_i
    cookie.last_publisher_match_by_subnetworks = {8 => match}

    cookie
  end

end
