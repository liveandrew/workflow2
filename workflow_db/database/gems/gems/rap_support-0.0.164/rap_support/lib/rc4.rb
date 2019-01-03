# this is a pseudo-random number generator that can be used where security is important
# random numbers are generated more slowly than Kernel.rand. much more slowly
# next_byte (gives # in [0,255]) is fast
module RC4
  SEED_DISCARD = 3072

  #reset its state. intended for testing
  def self.reset
    @@state = nil
  end
  
  self.reset

  #also calls Kernel#srand if weak val to get a larger key
  def self.srand(val=0)
    @@state = Array.new(256) if @@state.nil?
    if byte_factor(val).length < 8
      Kernel.srand(val)
      rand1 = Kernel.rand(2**128) 
    else
      rand1 = val
    end
    key = byte_factor(rand1)
    for i in (0...@@state.length)
      @@state[i] = i
    end
    j = 0
    for i in (0...@@state.length)
      j = (j + @@state[i] + key[i%key.length]) % @@state.length
      self.swap(i,j)
    end
    @@i = 0
    @@j = 0
    
    #discard the initial output since it is biased
    SEED_DISCARD.times { next_byte }
    #puts @@state.join(",")
    
  end
  
  def self.next_byte
    auto_init if @@state.nil?
    @@i = (@@i+1) % @@state.length
    @@j = (@@j + @@state[@@i]) % 256
    self.swap(@@i,@@j)
    return @@state[(@@state[@@i] + @@state[@@j]) % @@state.length]
  end
  
  #returns number in [0,max) unless max=0, where it returns a number in [0,1]
  def self.rand(max=0)
    auto_init if @@state.nil?
    big_num = 256*256*256-1
    return 1.0/big_num * self.rand(big_num+1) if max==0
    
    bytes_in_max = self.byte_factor(max)
    
    
    #strategy here: use a random number way way bigger than max given
    #the return will be the random number generated modded with the max, but
    #there is slight bias with last few numbers random number can possibly be.
    #So, loop until number chosen isn't one of those few numbers
    while(true)
      rnum = 0
      rnum_max = 0
      for i in (1..(bytes_in_max.length+2))
        b = self.next_byte
        rnum += b**i
        rnum_max += 256**i
      end
      if(rnum < rnum_max - (rnum_max % max))
        return rnum % max
      end
      #this line will be reached extremely rarely
    end
  end
  
  
  private
  
  #reads from /dev/urandom
  def self.auto_init
    val = 0
    File.open("/dev/urandom") do |f|
      for i in (1..20)
        b = f.getbyte
        val+=b**i
      end
    end
    self.srand(val)
  end
  
  def self.swap(i,j)
    t = @@state[i]
    @@state[i] = @@state[j]
    @@state[j] = t
  end
  
  #returns array of ints
  def self.byte_factor(num)
    num2 = num/256
    rem = num%256
    return [rem] if num2==0
    arr = byte_factor(num2)
    arr << rem
    return arr
  end

  
end


