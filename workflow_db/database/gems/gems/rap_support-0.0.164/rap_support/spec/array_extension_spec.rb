require File.dirname(__FILE__) + '/spec_helper'

describe Array, "extensions" do
  it "should to_proc correctly" do
    [{:k => "value", :other => "foo"}, {:k => "foo"}].map(&:k).should == ["value", "foo"]
  end
end
