$: << File.dirname(__FILE__)
require 'hank_constants'
require 'serialization_helper'
require 'smart_client'
require 'cookie_types/spruce_lib_service'

require 'rapleaf_types/new_person_data_constants'

module Rapleaf

  class BadArgumentError < StandardError; end

  class HankHelper

    include Rapleaf::Types::PersonData
    include Rapleaf::Types::NewPersonData
    include Rapleaf::Types::Spruce

    private
    def self.static_init
      @@client = nil
    end

    def self.connect
      @@client = SPRUCE_LIB_SERVICE
    end

    public

    def self.verify_argument_eid(eid)
      raise BadArgumentError.new("cannot take nil for argument 'eid'.") if eid.nil?
      raise BadArgumentError.new("expected a String for argument 'eid', but received #{eid.inspect}.") unless eid.instance_of? String
    end

    def self.verify_argument_heid(heid)
      raise BadArgumentError.new("cannot take nil for argument 'heid'.") if heid.nil?
      raise BadArgumentError.new("expected a String for argument 'heid', but received #{heid.inspect}.") unless heid.instance_of? String
    end

    def self.verify_argument_pin(pin)
      raise BadArgumentError.new("cannot take nil for argument 'pin'.") if pin.nil?
      raise BadArgumentError.new("expected a PIN for argument 'pin', but received #{pin.inspect}.") unless pin.instance_of? PIN
    end

    def self.verify_argument_nap(nap)
      raise BadArgumentError.new("cannot take nil for argument 'nap'.") if nap.nil?
      raise BadArgumentError.new("expected a NAP for argument 'nap', but received #{nap.inspect}.") unless nap.instance_of? PIN
      raise BadArgumentError.new("expected a NAP for argument 'nap', but received #{nap.inspect}.") unless nap.napkin?
    end

    # HANK HELPER
    def self.run_query(&block)
      if @@client.nil? then
        connect
      end
      response = nil
      begin
        response = block.call @@client
      rescue Thrift::TransportException, IOError => e
        connect
        response = block.call @@client
      end
      return process_response(response)
    end

    def self.process_response(response)
      if response.nil? || response.not_found?
        nil
      else
        response.response_value.get_value
      end
    end


    def self.eid_to_id_summ_pack(eid)
      verify_argument_eid(eid)
      run_query { |client| client.eid_to_id_summ_pack(eid) }
    end

    def self.eid_to_pin_and_owners(eid)
      verify_argument_eid(eid)
      id_summ_pack = eid_to_id_summ_pack(eid)
      id_summ_pack ? (id_summ_pack.pin_and_owners + (id_summ_pack.fringe_pin_and_owners || [])) : nil
    end

    def self.eid_to_naps(eid)
      eid_to_pin_and_owners(eid).map(&:pin).select(&:napkin?)
    end

    def self.eid_to_pins(eid)
      (eid_to_pin_and_owners(eid).select() { |pao| pao.owners.include?(DataPartner::RAPLEAF) }).map(&:pin)
    end

    # Generated Methods
    def self.eid_to_pz_pack(eid)
      verify_argument_eid(eid)
      run_query { |client| client.eid_to_pz_pack(eid) }
    end

    def self.active_eid_to_esp(eid)
      verify_argument_eid(eid)
      run_query { |client| client.active_heid_to_esp(Digest::MD5.digest(eid)) }
    end

    def self.active_heid_to_esp(heid)
      verify_argument_heid(heid)
      run_query { |client| client.active_heid_to_esp(heid) }
    end

    def self.eid_to_esp_waterfly(eid)
      verify_argument_eid(eid)
      run_query { |client| client.heid_to_esp_waterfly(Digest::MD5.digest(eid)) }
    end

    def self.heid_to_esp_waterfly(heid)
      verify_argument_heid(heid)
      run_query { |client| client.heid_to_esp_waterfly(heid) }
    end

    def self.pin_to_eid(pin)
      verify_argument_pin(pin)
      run_query { |client| client.pin_to_eid(pin) }
    end

    def self.radd_pin_to_pedigrees_and_values(pin)
      verify_argument_pin(pin)
      run_query { |client| client.radd_pin_to_pedigrees_and_values(pin) }
    end

    def self.redd_pin_to_pedigrees_and_values(pin)
      verify_argument_pin(pin)
      run_query { |client| client.redd_pin_to_pedigrees_and_values(pin) }
    end

    def self.nap_to_eids(nap)
      verify_argument_nap(nap)
      run_query { |client| client.nap_to_eids(nap) }
    end

    def self.nap_to_eid(nap)
      verify_argument_nap(nap)
      run_query { |client| client.nap_to_eid(nap) }
    end

    def self.naz_to_eids(naz)
      run_query { |client| client.naz_to_eids(naz) }
    end

    def self.preprocess_pin(pin)
      verify_argument_pin(pin)
      run_query { |client| client.preprocess_pin(pin) }
    end

    static_init
  end
end
