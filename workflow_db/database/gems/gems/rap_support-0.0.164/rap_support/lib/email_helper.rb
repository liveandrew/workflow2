module EmailHelper
  require 'rmail'
  require 'simpleidn'

  DISPOSABLE_EMAIL_ADDRESS_DOMAINS = Set.new
  File.open(File.dirname(__FILE__) + "/../misc/disposable_email_address_domains.txt").each {|line|
    DISPOSABLE_EMAIL_ADDRESS_DOMAINS.add(line.chomp) if line.count(".") == 1
  }

  # keep this list in sync with form.js list to ensure congruency between front- and back-end validations
  FREE_EMAIL_PROVIDERS = ["aol","hotmail","gmail","yahoo"].to_set

  def self.valid_email?(email_address)
    addr = RMail::Address.new(email_address)
    !addr.local.nil? &&
    !addr.local.empty? &&
    !addr.domain.nil? &&
    !addr.domain.empty? &&
    email_address.index(/,/).nil? &&      # Disallow commas
    addr.local.index(/[\s;:]/).nil? &&    # Check username piece for invalid chars
    valid_domain?(addr.domain) &&
    addr.local.size <= 64
  end

  def self.valid_domain?(domain)
    return false if !domain

    valid = false
    begin
      domain_ascii = SimpleIDN.to_ascii(domain)
      valid = !!domain_ascii.match(/^([0-9a-zA-Z](([0-9a-zA-Z]|-)*[0-9a-zA-Z])?\.)+(([a-zA-Z][a-zA-Z]+)|(xn--([0-9a-zA-Z]|-)*[0-9a-zA-Z]))$/) unless domain_ascii.nil?
    rescue Exception => e

    end
    valid
  end
  
  def self.valid_non_disposable_domain?(domain)
    valid_domain?(domain) && !DISPOSABLE_EMAIL_ADDRESS_DOMAINS.include?(domain)
  end

  def self.normalize(email_address)
    addr = RMail::Address.new(email_address.downcase)
    if !addr.local.empty? && !addr.domain.empty? then
      "#{addr.local}@#{addr.domain}"
    else
      nil
    end
  rescue
    nil
  end

  def self.email_correction_regexes
    [ /[^<]+\<([^>]+)\>/, /([^ ]+) +\([^)]+\)/, /'(.*)'/ ]
  end

  def self.secondary_domain_regex
    /([^%]+)%([^\.]+\.[^\@]+)@(.+)/
  end
  
  def self.is_disposable_email?(email)
    domain = email.split('@').last
    nonsub = domain.split('.')[-2, 2].join('.')
    # todo: does above actually work for gmail.co.uk for instance?
    !valid_non_disposable_domain?(nonsub)
  end

  def self.business_email?(email)
    return false if is_disposable_email?(email)
    domain = email.split('@').last.split(".").first
    return false if FREE_EMAIL_PROVIDERS.include? domain
    return true
  rescue Exception => e
    false
  end
  
end
