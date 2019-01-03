require File.dirname(__FILE__) + '/spec_helper'

describe Net::HTTP, "extensions" do
  it "should initialize open_timeout to 10" do
    Net::HTTP.new('http://127.0.0.1').open_timeout.should == 10
  end
end
