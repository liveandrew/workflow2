require 'cookie_jar'
require 'http_client'
require 'net/https'
require 'uri'
require 'base64'
require 'robots_txt'

# This class is used for direct look ups and look ups through squid proxies.
class IPHTTPClient < Rapleaf::HTTPClient

  attr_reader :proxy_host, :proxy_port, :proxy_id, :user_agent
  attr_accessor :cookies, :check_robots_txt
  
  # Returns a new IPHTTPClient object.
  #
  # *Parameters*:
  # * _options_ - (Hash) with possible keys of
  #   * proxy options
  #     * <tt>"proxy_id"</tt> - (Integer) the id number of the proxy to be used
  #     * <tt>"host"</tt> - (String) hostname of the proxy server (IP or domain)
  #     * <tt>"port"</tt> - (String) port the proxy server listens on
  #     * <tt>"username"</tt> - (String) username necessary to access the proxy (usually nil)
  #     * <tt>"password"</tt> - (String) password necessary to access the proxy (usually nil)
  #   * request options
  #     * <tt>"user_agent"</tt> - (String) use a specific user agent, if one is not passed a random one will be selected from 
  #       HTTPClient::USER_AGENTS
  #     * <tt>"referrer"</tt> - (String) referrer string used in first request
  # *Returns*:: _ip_http_client_
  def initialize(options = {})
    super
    @proxy_id   = options[:proxy_id] || options["proxy_id"]
    @check_robots_txt = options["check_robots_txt"]
    @proxy_host = options["host"]
    @proxy_port = options["port"]
    @proxy_username = options["username"] unless (options["username"].nil? || options["username"].empty?)
    @proxy_password = options["password"] unless (options["password"].nil? || options["password"].empty?)
    @user_agent = options["user_agent"] || DEFAULT_USERAGENT
    @http_class = Net::HTTP::Proxy(@proxy_host, @proxy_port, @proxy_username, @proxy_password)
    @cookies = CookieJar.new
    @referer = options["referrer"]
    @debug = ""
    @num_redirects = 0
  end
  
  # Performs a HTTP post to the specified url. Returns an array containing the 
  # url of the last page requested (redirects may occur) and the body of that 
  # page.
  #
  # *Parameters*:
  # * _url_ - (String) the url to request
  # * _post_data_ - (String) x=X&y=Y sets of values to POST
  # * _content_type_ - (String) the types of content we will accept in the response, when this is
  #   set it is usually because the post requests requires a certain content type to be 
  #   set for the call to return successfully or because we're imitating a javascript
  #   execution on the page (ajax calls for example)
  # * _custom_headers_ - (Hash) some requests require custom headers to complete successfully
  # *Returns*:: [<i>string:body</i>, <i>string:url</i>]
  # *Raises*:: ArgumentError, "HTTP redirect too deep" if the request tries to redirect more than 
  #            MAX_REDIRECTS times
  def post(url, post_data, content_type='application/x-www-form-urlencoded', custom_headers = {})
    perform_http_request(:post, url, post_data, content_type, custom_headers)
  end
  
  # Performs a HTTP get of the specified url. Returns an array containing the 
  # url of the last page requested (redirects may occur) and the body of that 
  # page.
  #
  # *Parameters*:
  # * _url_ - (String) the url to request
  # * _custom_headers_ - (Hash) some requests require custom headers to complete successfully
  # *Returns*:: [<i>string:body</i>, <i>string:url</i>]
  # *Raises*:: ArgumentError, "HTTP redirect too deep" if the request tries to redirect more than 
  #            MAX_REDIRECTS times
  def get(url, custom_headers = {})
    perform_http_request(:get, url, nil, nil, custom_headers)
  end
  
  # Performs a HTTP header request for the specified url. Returns an array containing the 
  # url of the last page requested (redirects may occur) and the body of that page.
  #
  # *Parameters*:
  # * _url_ - (String) the url to request
  # *Returns*:: [<i>string:body</i>, <i>string:url</i>]
  # *Raises*:: ArgumentError, "HTTP redirect too deep" if the request tries to redirect more than 
  #            MAX_REDIRECTS times
  def head(url)
    perform_http_request(:head, url)
  end
   
  private
  
  
  ##
  # type - either :get or :post for HTTP GET/POST request, or :head for HEAD request
  ##
  def perform_http_request(type, url, query_data=nil, content_type=nil, custom_headers={})
    raise ArgumentError, 'HTTP redirect too deep' if @num_redirects == MAX_REDIRECTS
    uri = URI.parse(url)
    uri.path = '/' if uri.path.nil? || uri.path.empty?
    
    raise RobotsTxtViolation unless can_fetch_url?(url)
    
    increment_http_request_count

    # determine SSL
    use_ssl = url =~ /^https/
    port = (use_ssl) ? 443 : 80
    @protocol = use_ssl ? 'https://' : 'http://'
    
    # instantiate http object
    http = @http_class.new(uri.host, port)
    http.use_ssl = use_ssl
    http.read_timeout = MAX_WAIT_TIME
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    
    # setup the headers
    @referer ||= @protocol + http.address
    all_headers = {'Cookie' => @cookies.cookies_for(uri),
                   'Accept-Encoding' => 'gzip, deflate',
                   'User-Agent' => @user_agent,
                   'Referer' => @referer}.merge(custom_headers)
    all_headers.delete('Cookie') if (all_headers['Cookie'].nil? || all_headers['Cookie'].empty?)
    all_headers.delete_if {|key,val| val.nil?}

    full_path = (uri.query.nil? || uri.query.empty?) ? uri.path : (uri.path+'?'+uri.query)
    response = nil

    # send the request, and time it out
    ::Timeout.timeout(MAX_WAIT_TIME) do
      if type == :get
        response = http.request_get(full_path, all_headers)
      elsif type == :post
        all_headers['Content-Type'] = content_type
        response = http.request_post(full_path, query_data, all_headers)
      elsif type == :head
        response = http.request_head(uri.path, all_headers)
      else
        raise "Unsupported request type: '#{type.inspect}'"
      end
    end
    
    update_bandwidth_data_counters(response, all_headers, query_data||"")
    
    # Inflate content if it's gzipped or deflated
    unless response.body.nil? || response.body == ''
      if ('gzip' == response.header['content-encoding'])
        response.instance_variable_set("@body", gz_read(response.body))
      elsif ('deflate' == response.header['content-encoding'])
        response.instance_variable_set("@body", Zlib::Inflate.inflate(response.body))
      end
    end
    
    # handle the response
    case response
    when Net::HTTPSuccess
      @referer = @protocol + http.address
      @cookies.parse_cookie_from(URI.parse(@protocol+uri.host), response.response['set-cookie']) 
      @num_redirects = 0
      
      return response.body, url
    when Net::HTTPRedirection
      @referer = @protocol + http.address
      @cookies.parse_cookie_from(URI.parse(@protocol+uri.host), response.response['set-cookie']) 
      @num_redirects += 1      
      get(derelativize(response['location'], uri.host), custom_headers.merge({"Referer" => url}))
    else
      if type == :get
        e = HTTPClientGetException.new
        e.response = response
        e.full_path = full_path
        raise e
      elsif type == :post
        e = HTTPClientPostException.new
        e.query_data = query_data
        e.response = response
        e.uri = uri
        raise e
      elsif type == :head
        e = HTTPClientHeadException.new
        e.response = response
        e.uri = uri
        raise e
      end
    end
  end
  
  
  def derelativize(url, host)
    #good to go
    if url =~ /^https?:\/\//
      return url
    elsif url =~ /^\// #relative to host
      return @protocol + host + url
    else
      return @protocol + host + '/' + url
    end
  end
  

  def skip_robots_check?(url)
    !!(url.to_s =~ /robots.txt$/ || !self.check_robots_txt)
  end


  # url - the string url we are checking robots.txt for
  def can_fetch_url?(url)

    # assume we can visit if we're not doing robots txt checks, and avoid infinite loop for actual robots.txt request
    return true if skip_robots_check?(url)
    uri = URI.parse(url)
    @robots_txt ||= RobotsTxt.new
    
    unless @robots_txt.has_rules_for_host?(uri.host)
      robots_txt_url = "http://#{uri.host}/robots.txt"
      robots_txt_body, url = perform_http_request(:get, robots_txt_url)
      @robots_txt.parse_robots_txt(robots_txt_body, uri.host)
    end
    
    @robots_txt.allows?(uri, user_agent) 
    
  rescue HTTPClientGetException => e
    # assume that robots.txt file isn't there
    true
  end
  
  
end
