require File.dirname(__FILE__) + '/spec_helper'
require 'rc4'


describe RC4 do
  before(:each) do
    RC4.reset
  end

  it "should auto-initialize its seed" do
    num = RC4.rand(10)
    num.should >= 0
    num.should < 10
    num2 = RC4.rand(1)
    num2.should == 0
    
  end
  
  it { RC4.should respond_to(:srand) }
  it { RC4.should respond_to(:next_byte) }
  it { RC4.should respond_to(:rand) }
end

describe RC4, " when #srand is called" do
  before(:each) do
    RC4.reset
  end

  it "should give same output with same seeds" do
    for i in (0...50)
      RC4.srand(i)
      nums = Array.new
      for j in (0..100)
        nums << RC4.next_byte
      end
      RC4.srand(i)
      for j in (0..100)
        b = RC4.next_byte
        b.should == nums[j]
      end
    end
  end
  
  it "should give different outputs with different seeds" do
    nums = Array.new
    for i in (1201...1305)
      RC4.srand(i)
      arr = []
      for j in (0..20)
        arr << RC4.next_byte
      end
      nums << arr
      
      for j in (0...nums.length-1)
        arr2 = nums[j]
        arr2.length.should == arr.length
        (arr2 == arr).should == false
      end
    end
  end
  
end

describe RC4, " when #next_byte is called" do
  before(:each) do
    RC4.reset
  end
  
  it "should always be between 0 and 255" do
    5000.times do
      b = RC4.next_byte
      b.should >= 0
      b.should <= 255
    end
  end
  
  it "should output every byte eventually" do
    gotten = Hash.new { |h, k| false }
    total = 0
    1000000.times do
      b = RC4.next_byte
      total+=1 if !gotten[b]
      gotten[b] = true
      break if total==256
    end
    total.should == 256
  end
end

describe RC4, " when #rand is called" do
  before(:each) do
     RC4.reset
  end
  
  it "should give a number in [0,1) with no arguments or 0 as argument" do
    r = RC4.rand
    r.should >= 0
    r.should < 1
    r = RC4.rand(0)
    r.should >= 0
    r.should < 1
  end
  
  it "should give an integer when called with a number greater than 0" do
    for i in (1...1000)
      r = RC4.rand(i)
      r.to_i.should == r
    end
  end
  
  it "should give an output in the right range" do
    for i in (1...1000)
      20.times do
        r = RC4.rand(i)
        r.should >= 0
        r.should < i
      end
    end
  end
  
  it "should average about (max-1)/2" do
    range = (1...1200)
    range.step(101) { |n|
      sum = 0
      target = (n-1)/2.0
      
      1500.times do
        sum+=RC4.rand(n)
      end
      avg = sum.to_f / 1500
      upper = 1.1*target
      lower = 0.9*target
      #puts "#{avg} #{target} #{lower} #{upper}"
      avg.should >= lower
      avg.should <= upper
    }
  end
  
end

