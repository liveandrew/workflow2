require File.dirname(__FILE__) + '/../spec_helper'
require 'thrift_service_wrapper.rb'

include Rapleaf

class FakeThriftService < ThriftServiceWrapper; end

describe ThriftServiceWrapper, "__sync_send" do
  
  before(:each) do
    @wrapper = ThriftServiceWrapper.new(nil, nil, nil, nil, nil, nil)
    @wrapper.stub!(:connect)
    @thrift_client = stub("ThriftClient", :foobar => 'result')
    @wrapper.instance_variable_set(:@thrift_client, @thrift_client)
  end
  
  it "should return result of call if no exceptions occur" do
    @wrapper.__sync_send(:foobar, []).should == 'result'
  end
  
  it "should return result of call if one exceptions occurs" do
    @thrift_client.should_receive(:foobar).once.ordered.and_raise(ThriftServiceWrapperTimeout.new("execution expired"))
    @wrapper.should_receive(:handle_sync_send_exception).and_return('result')
    @wrapper.__sync_send(:foobar, []).should == 'result'
  end
  
  it "should retry if internal timeout is exceeded" do
    @thrift_client.should_receive(:foobar).and_raise(ThriftServiceWrapperTimeout.new("execution expired"))
    @wrapper.should_receive(:handle_sync_send_exception)
    @wrapper.__sync_send(:foobar, [])
  end
  
  it "should re-raise exception if external timeout is exceeded" do
    @thrift_client.should_receive(:foobar).and_raise(Timeout::Error.new("execution expired"))
    @wrapper.should_receive(:close).once
    lambda { @wrapper.__sync_send(:foobar, []) }.should raise_error(Timeout::Error)
  end
  
  it "should raise exception if internal timeout is exceeded during retry" do
    @thrift_client.should_receive(:foobar).twice.and_raise(ThriftServiceWrapperTimeout.new("execution expired"))
    @wrapper.should_receive(:close).twice
    lambda { @wrapper.__sync_send(:foobar, []) }.should raise_error(ThriftServiceWrapperTimeout)
  end

end
