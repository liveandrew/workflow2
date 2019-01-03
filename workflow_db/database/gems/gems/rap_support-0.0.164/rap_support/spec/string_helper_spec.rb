require File.dirname(__FILE__) + '/spec_helper'
require File.dirname(__FILE__) + '/../core_ext/string_helper.rb'

describe String, " unsanitization" do

  it "should unescape escaped strings throughout" do
    str = "&amp; & hello &amp &quot; &lt; < < > > &gt; &lt &gt &amp;"
    str.unescape_html!
    str.should == '& & hello &amp " < < < > > > &lt &gt &'

    str = "aaabb &; &amp"
    str.unescape_html!
    str.should == "aaabb &; &amp"


  end

  it "should not unescape nested escaped strings" do
    str = "&&amp;lt;"
    str.unescape_html!
    str.should == "&&lt;"

    str = "abcd &amp; &&lt;amp;"
    str.unescape_html!
    str.should == "abcd & &<amp;"
    
  end
  
  it "should not modify original string without !" do
    str = "&amp;"
    str2 = str.unescape_html
    str.should == "&amp;"
    str2.should == "&"
  end

  it "should do full unescape" do
    str = "&&amp;lt;"
    str2 = str.full_unescape
    str2.should == "&<"
    str.should == "&&amp;lt;"
    str.full_unescape!
    str.should=="&<"
  end


end