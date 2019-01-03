class Hash

  # This method recursively strips tags off all String values in the hash.
  def strip_tags
    self.each {|key, value| self[key] = value.strip_tags if(value && value.respond_to?("strip_tags"))}
  end


  alias :old_to_s :to_s
  def to_s( method = nil )
    case method
    when :params
      self.map {|k,v| "#{k}=#{v}" }.join('&')
    when :cookies
      self.map {|k,v| "#{k}=#{v}" }.join(';')
    else
      self.old_to_s
    end
  end

  # Override deprecated id to use item in key :id
  def id
    id = self[:id]
    return id if id
    super
  end

  # Hash will accept keys as method names, and key= as setters
  # If value is a proc, it will (not) be evaluated.
  def method_missing(method, *args)
    if method.to_s[-1..-1] == '='
      self.send(:[]=, method.to_s[0..-2].to_sym, *args)
    else
      result = self.send(:[], method.to_sym)
      #result.respond_to?(:call) ? result.call(*args) : result
    end
  end
end
