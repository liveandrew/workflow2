require File.dirname(__FILE__) + '/../spec_helper'
require 'email_helper'

describe EmailHelper do

  it "should return false for invalid emails" do
    EmailHelper.valid_email?('banana').should be_false
  end

  it "should return false for emails with spaces in the domain" do
    EmailHelper.valid_email?('dayo@rap leaf.com').should be_false
  end

  it "should return false for emails with spaces in the userid" do
    EmailHelper.valid_email?('da yo@liveramp.com').should be_false
  end

  it "should return false for empty string" do
    EmailHelper.valid_email?('').should be_false
  end
  
  it "should return false for empty userid" do 
    EmailHelper.valid_email?('@gmail.com').should be_false
  end
  
  it "should return false for empty domain" do 
    EmailHelper.valid_email?('abhishek@').should be_false
  end
  
  it "should return false for userids with any of these chars: /[()\[\]\\;:,<>]/" do 
    EmailHelper.valid_email?('abhi:shek@gmail.com').should be_false
    EmailHelper.valid_email?('abhi,shek@gmail.com').should be_false
    EmailHelper.valid_email?('abhi>shek@gmail.com').should be_false
    EmailHelper.valid_email?('abhi]shek@gmail.com').should be_false
  end
  
  it "should return true for valid emails" do
    EmailHelper.valid_email?('neal@liveramp.com').should be_true
  end
  
  it "should return false for emails with multiple @" do
    EmailHelper.valid_email?('dayo@abhishek@liveramp.com').should be_false
  end

  it "should return false for emails with multiple @ and multiple domains" do
    EmailHelper.valid_email?('dayo@abhishek.com@abhishek.com').should be_false
  end

end
