require 'net/http'

module Net

  class HTTP
    
    alias :orig_initialize :initialize
   
    # Override HTTP#initialize to set a default @open_timeout to 10 secs. Original
    # initialize method sets @open_timeout to nil, causing connect to wait until 
    # able to open a TCPSocket.
    def initialize(*args)
      orig_initialize(*args)
      @open_timeout ||= 10
    end
    
  end

end
