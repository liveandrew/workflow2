require 'mixins/config_update_event_mixin'
require 'publisher'
require 'base64'

module CookieHelper

  if Rails.env.production?
    ENCRYPTION_KEY= ::Publisher.find_by_customer_id(781).encryption_key
  else 
    ENCRYPTION_KEY = "facefacefaceface"
  end
  ENCRYPTION_MODE = "bf-ecb"

  def self.parse_cookies(cookie_str, host=nil)
    cookies = Hash.new([])

    # split by comma with positive lookahead to the start of another cookie
    cookie_str.gsub(/(,([^;,]*=)|,$)/) { "\r\n#{$2}" }.split(/\r\n/).each do |fields|
      cookie = nil
      fields.split(/; ?/).each do |field|
        field_name, field_value = field.split('=', 2)
        field_value ||= ""
        field_value.gsub!(/^"|"$/, '')

        if !cookie
          begin
            cookie = CGI::Cookie.new({"name" => field_name, "value" => field_value})
          rescue Exception => e
            raise $!, "Error extracting cookie from cookie_str #{cookie_str}: #{$!}", $!.backtrace
          end 
        else
          case field_name.downcase
          when 'path' then cookie.path = field_value
          when 'domain' then cookie.domain = field_value
          when 'expires' then cookie.expires = Time.parse(field_value)
          when 'secure' then  cookie.secure = true
          end
        end
      end

      cookies[cookie.name] = cookie
    end
    cookies
  end

  def self.decrypt_cookie(cookie_str, thrift_cookie)
    begin
      ser = CookieHelper.get_plaintext(nil, cookie_str)
      SerializationHelper.deserialize(thrift_cookie, ser)
      return thrift_cookie
    rescue Exception => e
      raise "Could not decrypt cookie #{cookie_str}.\n#{e.pretty}"
    end
  end

  def self.decrypt_cookie_base64(cookie_str, thrift_cookie)
    begin
      ser = CookieHelper.get_plaintext_base64(nil, cookie_str)
      SerializationHelper.deserialize(thrift_cookie, ser)
      return thrift_cookie
    rescue Exception => e
      raise "Could not decrypt cookie #{cookie_str}.\n#{e.pretty}"
    end
  end

  def self.get_ciphertext_and_iv_hex(plaintext, mode=ENCRYPTION_MODE, key=ENCRYPTION_KEY)
    key = wrap_key(mode, key)
    cipher = OpenSSL::Cipher::Cipher.new(mode).encrypt
    cipher.key_len = key.length
    cipher.key = key
    iv = cipher.random_iv.to_hex

    encrypted = ((cipher.update(plaintext)) + cipher.final).to_hex
    [encrypted, iv]
  end

  def self.get_ciphertext_and_iv_base64(plaintext, mode=ENCRYPTION_MODE, key=ENCRYPTION_KEY)
    key = wrap_key(mode, key)
    cipher = OpenSSL::Cipher::Cipher.new(mode).encrypt
    cipher.key_len = key.length
    cipher.key = key
    iv = cipher.random_iv.to_hex

    encrypted = Base64.encode64((cipher.update(plaintext)) + cipher.final).gsub("\n", "")
    [encrypted, iv]
  end

  private

  def self.get_plaintext_base64(iv, ciphertext, mode = ENCRYPTION_MODE, key = ENCRYPTION_KEY, retried = false)
    return "Unable to Decrypt: ciphertext is nil" if ciphertext.nil? # Decrypt works on empty strings but not nil.

    cipher = OpenSSL::Cipher::Cipher.new(mode).decrypt
    cipher.key_len = key.length
    cipher.key = key
    cipher.iv = iv if iv

    plaintext = nil

    begin  ## Uncomment this to support mcrypt encryption
      plaintext = cipher.update(Base64.decode(ciphertext))
      plaintext += cipher.final unless retried
    end

    tk.finish if @timer

    plaintext
  end

  def self.get_plaintext(iv, ciphertext, mode = ENCRYPTION_MODE, key = ENCRYPTION_KEY, retried = false)
    return "Unable to Decrypt: ciphertext is nil" if ciphertext.nil? # Decrypt works on empty strings but not nil.

    cipher = OpenSSL::Cipher::Cipher.new(mode).decrypt
    cipher.key_len = key.length
    cipher.key = key
    cipher.iv = iv if iv

    plaintext = nil

    begin  ## Uncomment this to support mcrypt encryption
      plaintext = cipher.update([ciphertext].pack("H*"))
      plaintext += cipher.final unless retried
    rescue Exception => e
      if retried
        raise e
      else
        # retry with padded ciphertext for mcrypt
        plaintext = get_plaintext(mode, key, iv, ciphertext + '1'*8, true).strip
        plaintext += "<br/><br>If text above looks good, please inform the engineers that this is an MCrypt publisher. If it doesn't, something is wrong with the encryption."
      end
    end

    tk.finish if @timer

    plaintext
  end

  def self.wrap_key(mode, key)
    if (mode =~ /^des-ede/) # key needs to be 24bytes
      key[0,24]
    elsif (mode =~ /^des/) # key needs to be 8bytes
      key[0,8]
    else
      key[0,16]
    end
  end
end
