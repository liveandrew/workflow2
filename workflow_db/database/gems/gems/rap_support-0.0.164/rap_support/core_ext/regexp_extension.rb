class Regexp
  # Makes it so you can to a &/bla/ parameter to an iterator and it will 
  # perform a match against that regex and return the value if it matches, 
  # nil otherwise. If there is a group in the regex, it will return the first
  # group instead.
  #
  # Examples:
  # self.methods.detect(&/^a/) 
  #   => All methods starting with 'a'
  #
  # self.methods.map(&/^(a.).*/) 
  #   => First two letters of all methods starting with a
  def to_proc
    Proc.new { |arg| 
      (arg =~ self) ? ($1 || arg) : nil
    }
  end
end
