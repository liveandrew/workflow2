# This is an extension to Ruby's string class that helps us with our
# random string issues.



#Adding these to NilClass so we can call unescape at will and not worry about if it was provided or not
class NilClass
  def full_unescape!
   self
  end
  
  def full_unescape
    self
  end
  
  def unescape_html!
    self
  end

  def unescape_html
    self
  end
end

class String

  public
  
  # good_value is the result of converting a block to a Proc object
  # called like so:
  #
  # 	get_unique_random_string(8) | code block that takes a string argument and a boolean, and
  #       alters the boolean|
  #
  
  
  # Use when you want to enclose a string with a char(s)
  def enclose_with( char )
    "#{char}#{self}#{char}"
  end
  
  
  # gsub out control characters (see http://www.asciitable.com/)
  def remove_control_characters!
    self.gsub!(/[[:cntrl:]]/, '')
  end
  
  def remove_control_characters
    self.gsub(/[[:cntrl:]]/, '')
  end
  
  
  def full_unescape!
    self.replace(self.full_unescape)   
    self 
  end
  
  #unescapes the string over and over until fixed point is reached
  def full_unescape
    str = self
    while(true)
      str2 = str.unescape_html
      return str if str2 == str
      str = str2 
    end
  end
  
  def unescape_html!
    self.replace(self.unescape_html)
    self
  end

  #unescapes &amp; &quot; &gt; &lt; &#x27;
  #This function does ONE pass through the string (meant to be inversion of html_escape function)
  #For recursive unescape until fixed point is reached, use full_unescape
  def unescape_html
    mapping = { '&amp;' => '&', '&lt;' => '<', '&gt;' => '>', '&quot;' => '"', '&#x27;' => "'" }
    to_replace = []
    str = String.new(self)

    mapping.each_key do |s|
      ind = 0
      while(true)
        ind = str.index(s,ind)
        break unless ind
        to_replace << [s, ind]
        ind+=s.length
      end
    end
    #sort on decreasing indices, so as string is modified, future indices remain the same
    to_replace.sort! do |a,b|
      ret = 1 if a[1] < b[1]
      ret = -1 if a[1] > b[1]
      ret
    end
    to_replace.each do |arr|
      s = arr[0]
      ind = arr[1]
      str.slice!(ind,s.length)
      str.insert(ind,mapping[s])
    end
    str
  end
  
  def String.get_unique_random_string(length, method=Kernel, &good_value)
  	valid_string=false
  	while (!valid_string)
  	    candidate = get_random_string(length, method)
  	    valid_string = good_value.call(candidate)
  	end
  	candidate
  end

  
  def String.get_random_string(length, method=Kernel)
  	string = ""
  	1.upto(length) { |i| string << @@rapleaf_chars[method.rand(@@rapleaf_chars_size)] }
  	string
  end
  
  # returns the portion of an email address just before the '@' symbol
  def email_username
    i = index('@')
    self[0, i] if i
  end

  #method should define "rand" function
  def String.get_random_number_string(length, method=Kernel)
  	string = ""
  	1.upto(length) { |i| string << @@rapleaf_digits[method.rand(@@rapleaf_digits_size)] }
  	string
  end
  
  def String.get_unique_random_number_string(length, method=Kernel, &good_value)
   	valid_string=false
   	while (!valid_string)
   	    candidate = get_random_number_string(length, method)
   	    valid_string = good_value.call(candidate)
   	end
   	candidate
   end
  
  def convert_html_unicode_to_utf8
    self.gsub(/&#(x?[0-9a-fA-F]+);/) do |m|
      if 'x' == $1.at(0)
        code_point = $1.sub('x','').hex
      else
        code_point = $1.to_i
        next if 0 == code_point
      end
      [code_point].pack("U")
    end
  end
  
  # Hex is the "nonce" (e.g. "bd955b1600000000")
  def to_bytes
    [self].pack("H*")
  end

  # Bytes is a string (e.g. "\275\225[\026\000\000\000\000")
  def to_hex
    self.unpack("H*")[0]
  end

  private
  
  #WEG--
  #  Variables prefixed with rapleaf_ because, after all, we're adding class variables
  #  to String (we don't own the definition of String and want to avoid name conflicts)
  #END--WEg
  @@rapleaf_chars = ("a".."z").to_a + ("A".."Z").to_a + ("0".."9").to_a
  @@rapleaf_chars_size = @@rapleaf_chars.size
  @@rapleaf_digits = ("0".."9").to_a
  @@rapleaf_digits_size = @@rapleaf_digits.size
end
