$: << File.expand_path(File.dirname(__FILE__))
require "thrift"
require "thread"
require "timeout"

class ThriftServiceWrapperTimeout < Timeout::Error; end

module Rapleaf
  class ThriftServiceWrapper
    # Default timeout is 10 seconds.
    # After 1 timeout the request will be tried once more, again with the same timeout.
    def initialize(klass, protocol_factory, host, port, framed = true, timeout = 10)
      @klass, @protocol_factory, @host, @port, @framed, @timeout = klass, protocol_factory, host, port, framed, timeout
      @mutex = Mutex.new
    end

    # generate methods calls (with retry) to the thrift client as they are needed
    # for a list of available methods please refer the thrift Client
    def method_missing(method, *args)
      raise NoMethodError, "#{@klass}.#{method} method does not exist" unless @klass::Client.instance_methods.map(&:to_s).include?(method.to_s)
      self.class.class_eval do # this is necessary because define_method is a private method of the class
        define_method(method) do |*params|
          __sync_send(method, params)
        end
      end
      
      # call the method we just defined
      send(method, *args)
    end
    
    def __sync_send(method, params)
      @mutex.synchronize do
         @error = false
         begin
           Timeout::timeout(@timeout, ThriftServiceWrapperTimeout) do
             connect
             @thrift_client.send(method, *params)
           end
           
         rescue ThriftServiceWrapperTimeout # rescue thrift wrapper timeout and retry once
           handle_sync_send_exception(method, params)
           
         rescue Timeout::Error => e # for any other timeout, close transport and re-raise
           close
           raise e
           
         rescue Exception # rescue all other exceptions and retry once
           handle_sync_send_exception(method, params)
           
         ensure
           close if @error
         end
       end
    end
    
    # Close transport and clear out the instance variable so it wont be reused
    def close
      if @transport
        @transport.close rescue nil
        @transport = nil
      end
    end
    
    ###################
    # Internal methods

    private

    def connect
      unless @transport && @transport.open?
        @transport = Thrift::Socket.new(@host, @port.to_i)
        @transport = Thrift::FramedTransport.new(@transport) if @framed
        @transport = Thrift::BufferedTransport.new(@transport) unless @framed
        protocol = @protocol_factory.get_protocol(@transport)
        @thrift_client = @klass::Client.new(protocol)
        @transport.open
      end
    end
    
    
    # retry making the call
    def handle_sync_send_exception(method, params)
      @error = true
      close
      val = Timeout::timeout(@timeout, ThriftServiceWrapperTimeout) do
        connect
        @thrift_client.send(method, *params)
      end
      @error = false
      val
    end

  end
  
  
end
