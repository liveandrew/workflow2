require File.dirname(__FILE__) + '/spec_helper'

describe Object, "when attempting to execute a block" do
  it "should return value of block upon success" do
    retries = return_val = 1
    attempt(retries) { return_val }.should == return_val
  end
  
  it "should sleep between retries if specified" do
    self.should_receive(:sleep).with(1)
    attempt(2, 1) { raise ArgumentError } rescue nil
  end
  
  it "should give up and re-raise exception if all retry attempts fail" do
    self.stub!(:sleep)
    lambda { (attempt(2, 1) { raise ArgumentError }) }.should raise_error(ArgumentError)
  end
end