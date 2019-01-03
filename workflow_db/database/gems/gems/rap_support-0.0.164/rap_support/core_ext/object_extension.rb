class Object
  
  def to_bool
    # Forces return of a boolean value
    self ? true : false
  end

  def to_html
    to_s
  end

  def deep_copy
    Marshal.load(Marshal.dump(self))
  end
  
   # The hidden singleton lurks behind everyone
  def metaclass; class << self; self; end; end
  def meta_eval &blk; metaclass.instance_eval &blk; end

  # Adds methods to a metaclass
  def meta_def name, &blk
    meta_eval { define_method name, &blk }
  end

  # Defines an instance method within a class
  def class_def name, &blk
    class_eval { define_method name, &blk }
  end

  # Functional let
  def let *args
    yield *args
  end

  def attempt(max_attempts, sleep_between_attempts = 0)
    cattempts = 0
    loop do
      begin
        return yield
      rescue Exception => e
        cattempts += 1
        raise e if cattempts >= max_attempts
        sleep sleep_between_attempts
      end
    end
  end
end
