class String
  def prefixes(delimiter='/')
    return ['/'] if split(delimiter).length==0
    segments = split(delimiter)
    retval = (1..segments.length).collect { |i| segments[0..-i].join(delimiter) }
    retval.map!{|r| (r.size == 0) ? '/' : r}
    retval
  end
  def suffixes(delimiter='/')
    segments = split(delimiter)
    retval = (1..segments.length).collect { |i| segments[(i-1)..-1].join(delimiter) }
    retval.map!{|r| (r.size == 0) ? '/' : r}
    retval
  end
end

class CookieJar
  def initialize
    @jar = {}
  end
  
  def parse_cookie_from(uri,s)
    return unless s.is_a? String and s.length > 2
    cookies = s.gsub(/, ([^\d])/,';;\1').split(';;')
    cookies.each { |cookie|
      name_value,*options = cookie.split(';')
      name,value = name_value.split('=', 2)
      acceptable_protocols,domain,path,expires = ['http','https'],uri.host,uri.path,nil
      domain,path = uri.host,uri.path
      options.each { |option| case option
                              when /[Ee]xpires=.+?, (..-...-..(..)? ..:..:.. GMT)/
                                expires = DateTime.parse($1,:guess_year)
                              when /[Pp]ath=(.*)/                     
                                path = $1
                              when /[Dd]omain=([^\s]*)/                      
                                domain = $1
                              when /[Ss]ecure/                                
                                acceptable_protocols = ['https']
                              end
        path = '/' if path.length==0 || path.nil?}
      (@jar[domain] ||= {})[path] ||= []
      unless value.nil? || value.empty?
        @jar[domain][path] = @jar[domain][path].delete_if { |x| x[0] == name }
        @jar[domain][path] << [name,value]
      end
      #((@jar[domain] ||= {})[path] ||= []) << [name,value] #,expires, acceptable_protocols]
    }
  end
  
  def cookies_for(uri)
    result = {}
    host = (uri.host.split('.')[-2..-1])
    domains = @jar.keys.select{|d| d.split('.')[-2..-1] == host}
    
    domains.each { |domain|
      uri.path = '/' if uri.path.length==0 || uri.path.nil?
      uri.path.prefixes.each { |path|   
        @jar[domain][path].each { |name,value| 
          (result[name] ||= []) << value 
        } if @jar[domain].has_key? path
      } if @jar.has_key? domain
    }
    def result.to_s
      keys.collect { |name|
        self[name].collect { |value| "#{name}=#{value}" }
      }.flatten.join('; ')
    end
    result.to_s
  end
  
  def remove_cookie(domain, path, cookie)
    @jar[domain][path].delete_if { |a,b| a == cookie }
  end
end
