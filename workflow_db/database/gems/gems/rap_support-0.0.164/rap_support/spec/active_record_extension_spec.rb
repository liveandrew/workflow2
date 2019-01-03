require File.dirname(__FILE__) + '/spec_helper'
require 'active_record'

# Scaffold enum test class
class EnumTestClass < ActiveRecord::Base
  def initialize
  end
  
  def before_enumerable_update(val)
  end
  
  def method_missing(method, *args)
    if method.to_s.eql? "enumerable" 
      @enumerable
    elsif method.to_s.eql? "enumerable="
      @enumerable = args[0]
    end
  end
  
  # override ActiveRecord::Base.columns_hash so that 
  # it doesn't need a database connection
  def self.columns_hash
    {}
  end
end

# Include the active record enum extension
require File.dirname(__FILE__) + "/../core_ext/active_record_extension"

describe ActiveRecord, " enum" do
  before :all do
    @enum_name = :enumerable
    @enum_hash = { :first => 0, :second => 1 }
    @invalid_val = -1
    @invalid_sym = :invalid
    EnumTestClass.enum @enum_name, @enum_hash
  end
  
  before :each do
    @instance = EnumTestClass.new
  end
  
  it "should return enum definition when calling <enum_name> plural method" do
    EnumTestClass.send(@enum_name.to_s.pluralize).should == @enum_hash
  end
  
  it "should return correct enum name for i_to_<enum_name> with valid integer value " do
    @enum_hash.each { |sym, val| EnumTestClass.i_to_enumerable(val).should == sym }
  end

  it "should return nil for i_to_<enum_name> with invalid integer value " do
    EnumTestClass.i_to_enumerable(@invalid_val).should be_nil
  end
  
  it "should return correct enum value for <enum_name>_to_i with valid enum name " do
    @enum_hash.each { |sym, val| EnumTestClass.enumerable_to_i(sym).should == val }
  end
  
  it "should raise error for <enum_name>_to_i with invalid enum name" do 
    lambda { EnumTestClass.enumerable_to_i(@invalid_sym) }.should raise_error
  end
  
  it "should return correct integer for valid_<enum_name>? with valid symbol " do
    @enum_hash.each { |sym, val| EnumTestClass.valid_enumerable?(sym).should be_true }
  end
  
  it "should return correct name for valid_<enum_name>? with valid value" do
    @enum_hash.each { |sym, val| EnumTestClass.valid_enumerable?(val).should be_true }
  end
  
  it "should return false for valid_<enum_name>? with invalid value" do
    EnumTestClass.valid_enumerable?(@invalid_val).should be_false
  end
  
  it "should return false for valid_<enum_name>? with invalid symbol" do
    EnumTestClass.valid_enumerable?(@invalid_sym).should be_false
  end
  
  it "should return true for <enum_name>_<enum_sym>? when enum_name has enum_sym value" do
    # 
    @enum_hash.each do |sym, val| 
      @instance.enumerable = val
      @instance.send("enumerable_#{sym.to_s}?").should be_true
    end
  end
  
  it "should return false for <enum_name>_<enum_sym>? when enum_name does not have enum_sym value" do
    @enum_hash.each do |sym, val|
      @instance.enumerable = val
      @enum_hash.each do |test_sym, test_val|
        unless test_sym.eql? sym
          @instance.send("enumerable_#{test_sym.to_s}?").should be_false
        end
      end
    end
  end
  
  it "should assign correctly for <enum_name>= with valid integer" do
    @enum_hash.each do |sym, val|
      @instance.enumerable = val
      @instance.enumerable.should == val
    end
  end
  
  it "should assign correctly for <enum_name>= with valid enum_sym value" do
    @enum_hash.each do |sym, val|
      @instance.enumerable = sym
      @instance.enumerable.should == val
    end
  end
  
  it "should raise error for <enum_name>= on invalid integer" do
    lambda { @instance.enumerable = @invalid_val }.should raise_error("Invalid value for enumerable: #{@invalid_val}")
  end
  
  it "should raise error for <enum_name>= on invalid enum_sym value" do
    lambda { @instance.enumerable = @invalid_sym }.should raise_error("Invalid value for enumerable: #{@invalid_sym.to_s}")
  end
  
  it "should call before_<enum_name>_update if defined for <enum_name>=" do
    @instance.should_receive :before_enumerable_update
    @instance.enumerable = @enum_hash.values.first
  end
  
  # TODO Stubbing ActiveRecord::Base#method_missing results in method_missing being recursively called until StackError is thrown
  it "should convert enum_sym to integer values in args when called with find_by_<enum_name>" 
  it "should convert enum_sym to integer values in args when called with find_or_create_by_<enum_name>"  
  it "should convert enum_sym to integer values in args when called with find_all_by_<enum_name>" 
end