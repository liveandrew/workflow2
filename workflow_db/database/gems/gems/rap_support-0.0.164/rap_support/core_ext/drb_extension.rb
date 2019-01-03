module DRb
  class DRbMessage
    private

    def make_proxy(obj, error=false)
      raise "You are creating an implicit proxy. Please refactor towards a clear network interface.  #{obj.inspect} at #{caller.join("\n")}."
    end
  end
end
