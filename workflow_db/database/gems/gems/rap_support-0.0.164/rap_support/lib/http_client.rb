require 'zlib'

class HTTPClientGetException < Exception
  
  attr_accessor :full_path, :response, :url, :all_headers
  
  def pretty
    "response: #{self.response.inspect}, path: #{self.full_path}, code: #{self.response.code}"
  end
  
end

class HTTPClientPostException < Exception
  
  attr_accessor :query_data, :response, :uri
  
  def pretty
    "response: #{self.response.inspect}, query_data: #{self.query_data.inspect}, uri: #{self.uri.inspect}, code: #{self.response.code}"
  end
  
end

class HTTPClientHeadException < Exception
  
  attr_accessor :response, :uri

  def pretty
    "response: #{self.response.inspect}, uri: #{self.uri.inspect}"
  end

end

class PHPHTTPClient500Error < Exception 
  
  attr_accessor :php_client
  
  def pretty
    self.class.name + " : " + self.php_client.inspect
  end
  
end

# Performs HTTP and HTTPS requests and keeps track of the state required for those requests.  Most HTTPClients are configured to use a Proxy
# in order to make their requests.  The class of client chosen (IPHTTPClient or a PHPHTTPClient) depends on the type of proxy (squid or PHP 
# respectively) being used.

module Rapleaf
  class HTTPClient
  
    DEFAULT_USERAGENT = "Mozilla/4.0 (compatible; Rapleafbot/1.0; +http://www.rapleaf.com/contact.html;)"
  
    MAX_REDIRECTS = 10
  
    MAX_WAIT_TIME = 120 # 2 minutes
  
    attr_reader :incoming_data_counter, :outgoing_data_counter, :init_options, :http_request_count
  
    # Returns an instance of the HttpClient type specified by the "class_name" key of the options hash.
    #
    # *Parameters*:
    # * _options_ - (Hash) with possible keys of
    #   * <tt>:class_name</tt> - (String) Class Name to be used to instantiate the right HTTPClient Object [IPHTTPClient, PHPHTTPClient, 
    #     PHPHTTPClientV2]
    #   * <tt>:proxy_host</tt>* - (String) IP Address of proxy server to use
    #   * <tt>:proxy_port</tt>* - (String) Port Number of the port on the proxy_host to use to access the proxy server
    #   * <tt>"host"</tt>* - (String) Host Name of the php host
    #   * <tt>"path""</tt>* - (String) Path where the php_test.php (php proxy) file exists on the host machine
    #   <tt></tt>* denotes that the key is dependent on the proxy type
    # *Returns*:: _http_client_
    # *Raises*:: "Unknown Instance Name" if "class_name" is not passed as a key in the options hash
    def self.get_instance(options = {})
      # Allow for symbols as keys
      options.each {|k, v| options[k.to_s] = v if k.is_a?(Symbol)}
      raise "Unknown Instance Name" unless options["class_name"]
      http_class = Module.const_get(options["class_name"])
      http = http_class.new(options)
      http.instance_variable_set(:@init_options, options)
      http
    end
  
    # Returns a new HttpClient instance with <tt>@incoming_data_counter</tt>, <tt>@outgoing_data_counter</tt>, and
    # <tt>@http_request_count = 0</tt> all set to 0.
    #
    # *Returns*:: _http_client_
    def initialize(options = {})
      @first_request_done = false
      @incoming_data_counter = 0
      @outgoing_data_counter = 0
      @http_request_count = 0
    end
  
    # Sets <tt>@incoming_data_counter</tt>, <tt>@outgoing_data_counter</tt>, and <tt>@http_request_count</tt> all to 0.
    #
    # *Returns*:: 0
    def reset_data_counters
      @incoming_data_counter = 0
      @outgoing_data_counter = 0
      @http_request_count = 0
    end
  
    # Sets <tt>@referrer</tt> to "".
    #
    # Was used for Perfspot ExtendedSearchSite which required a nil referrer for many of their calls to succeed.
    #
    # *Returns*:: ""
    def clear_referer!
      @referer = ""
    end
  
    # Calls the cookies_for method with parameter _uri_ on the CookieJar instance held in <tt>@cookies</tt>.
    #
    # *Returns*:: _string_
    def cookies_from_uri(uri)
      @cookies.cookies_for(uri)
    end
  
    # Calls the parse_cookie_from method with parameters _uri_ and _s_ on the CookieJar instance held in <tt>@cookies</tt>.
    #
    # *Returns*:: _array_
    def parse_cookie_from(uri,s)
      @cookies.parse_cookie_from(uri,s)
    end

    # Sets <tt>@cookies</tt> to a new CookieJar instance.
    #
    # *Returns*:: _cookie_jar_
    def clear_cookies!
      @cookies = CookieJar.new
    end
  
    # Increments <tt>@http_request_count</tt> to reflect an additional request done.
    # Subclasses of HTTPClient should call this method every time an http request is
    # done.
    def increment_http_request_count(amount = 1)
      @http_request_count += amount
    end
  
    # Follows redirections given in javascript or meta tags in _body_.
    #
    # *Parameters*:
    # * _url_ - (String) the url of the page in _body_
    # * _body_ - (String) a page of webcontent
    # * _max_ - (Integer) the maximum number of redirects that should be followed, default = 5
    # *Returns*:: [<i>string:body</i>, <i>string:url</i>]
    def follow_body_redirects(url, body, max=5)
      max.times do 
        break unless
            body =~ /document\.location\.href\s*=\s*'([^']+)'/ or 
            body =~ /document\.location\.href\s*=\s*"([^"]+)"/ or 
            body =~ /(?:top|window)\.location\.replace\(\s*"([^"]+)"\)/ or
            body =~ /(?:top|window)\.location\.replace\(\s*'([^']+)'\)/ or 
            body =~ /http-equiv=.refresh.\s*content="\d+;URL=([^"]+)"/i or
            body =~ /http-equiv=.refresh.\s*content='\d+;URL=([^']+)'/i
        
        redirect_url = get_absolute_url($1.gsub('\\', ''), url)
        body, url = attempt(3,1){ get(redirect_url) }
      end
      return body, url
    end
  
    # Returns the full path of a relative link based on the url of the page linked from.
    #
    # *Parameters*:
    # * _url_ - (String) url (href) from the link
    # * _oldurl_ - (String) url of the current page
    # *Returns*:: _string_
    def get_absolute_url(url, oldurl)
      return url if url =~/^https?\:/ 
      uri = URI.parse(oldurl)
      if url =~ /^\//   # this is relative to host
        path = "#{uri.host}#{url}"
      else  # this is relative to current directory
        uri.path = '/' if uri.path.blank?
        path = "#{uri.host}#{uri.path.sub(/[^\/]*$/, url)}"
      end
      "#{uri.scheme}://#{path}"
    end
  
    private
  
  
    # Determine how much incoming and outgoing bandwidth was used based on the http response and what data was sent
    def update_bandwidth_data_counters(response, headers, data="")
      # the last term adds the 3 times the number of headers in order to account for the ': ' and newline for each header
      http_request_size = data.size + headers.to_s.size + 3*headers.size
      http_response_size = response.body.size + response.header.to_hash.to_s.size + 3*response.header.to_hash.size
    
      update_counters(http_request_size, http_response_size)
    end
  
    def update_counters(http_request_size, http_response_size)
      unless @first_request_done # add bandwidth for TCP handshake and disconnect only once per connection
        # Init Connection: 78 for SYN, 66 for ACK
        # Close Connection: 66 for each of ACK, FINACK, FINACK, ACK, ACK
        @outgoing_data_counter += 474
        # Init Connection: 78 for SYNACK
        # Close Connection: 66 for each of ACK, FINACK, ACK, FINACK
        @incoming_data_counter += 342
      end
      @first_request_done = true
    
      # 1440 is assumed to be the max data size per packet (for the http data).
      num_request_packets = (http_request_size/1440.0).ceil
      num_response_packets = (http_response_size/1440.0).ceil
    
      # 66 bytes of headers for each request/response packet + 66 bytes for the ACK packet in response to each request/response packet
      # 66 bytes of headers are made up of 32 for TCP, 20 for IP, and 14 for Ethernet II
      overhead = (num_request_packets*66) + (num_response_packets*66)
      @outgoing_data_counter += http_request_size + overhead
      @incoming_data_counter += http_response_size + overhead
    end
  
  
    # Decompresses a string of gzip data in a "best effort" manner.
    # If errors occur during decompression, this method returns all
    # the data that it successfully decompressed, without reporting
    # the error.
    # This is the same gzip behavior that most web browsers and other
    # HTTP clients have.
    def gz_read(data)
      gz_reader = Zlib::GzipReader.new(StringIO.new(data))
      output = ""
      read_size = data.size * 2
      while (read_size > 0) do
        begin
          chunk = gz_reader.read(read_size)
          if chunk.nil?
            read_size = read_size/2
          else
            output += chunk
          end
        rescue Zlib::GzipFile::Error
          read_size = read_size/2
        end
      end
      output
    end

  end
end