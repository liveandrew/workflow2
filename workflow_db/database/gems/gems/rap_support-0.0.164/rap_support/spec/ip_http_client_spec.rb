require File.dirname(__FILE__) + '/spec_helper'
require 'ip_http_client'

describe IPHTTPClient do
  
  it { IPHTTPClient.new.should respond_to(:clear_cookies!) }
  
end


describe IPHTTPClient, "when #clear_cookies! is called" do
  before(:each) do
    @ip_http = IPHTTPClient.new
  end
  
  it "should remove all cookies" do
    uri = stub('URI', :host => 'rapleaf.com', :path => '/')
    str = 'key=val'
    @ip_http.parse_cookie_from(uri, str)
    @ip_http.cookies_from_uri(uri).should_not be_empty
    @ip_http.clear_cookies!
    @ip_http.cookies_from_uri(uri).should be_empty
  end
  
end


# TODO: Should probably move to a robots_txt spec
describe IPHTTPClient, "when initialized with check_robots_txt" do
  before do
    @http = IPHTTPClient.new('check_robots_txt' => true, 'user_agent' => "Rapleafbot")
    @robots_response = stub('/robots.txt response', :body => "", :response => {}, :header => {}) 
    Net::HTTPSuccess.stub!(:===).and_return(true)
    @test_response = stub('/test response', :body => "", :response => {}) 
    @mock_http_client = stub('HttpClientStub', :address => "", :verify_mode= => nil, :read_timeout= => nil, :use_ssl= => nil, :request_get => @test_response, :request_post => @test_response)
    @http_class = stub('HTTPClass', :new => @mock_http_client)
    @http.instance_variable_set(:@http_class, @http_class)
    @http.stub!(:increment_http_request_count)
    @http.stub!(:update_bandwidth_data_counters)
  end
  
  it "should fetch for robots.txt file on domain of http request" do
    @mock_http_client.should_receive(:request_get).with("/robots.txt",{"User-Agent"=>"Rapleafbot", "Accept-Encoding"=>"gzip, deflate", "Referer"=>"http://"}).and_return(@robots_response)
    @http.get("http://rapleaf.com/test")
  end
  
  it "should make request if robots.txt does not exist" do
    Net::HTTPSuccess.stub!(:===).and_return(false,true) # this will make us act similar to a 404 first (for robots.txt), then a 200 for /test
    @mock_http_client.should_receive(:request_get).with("/test",{"User-Agent"=>"Rapleafbot", "Accept-Encoding"=>"gzip, deflate", "Referer"=>"http://"}).and_return(@test_response)
    @http.get("http://rapleaf.com/test")
  end
  
  it "should make request if there is no directive RapleafBot or * User-Agent" do
    robots_txt_body = <<-STR
    User-agent: NextGenSearchBot
    Disallow: /

    User-agent: Spock
    Disallow: /
    STR
    @robots_response.stub!(:body).and_return(robots_txt_body)
    @mock_http_client.stub!(:request_get).and_return(@robots_response)
    @mock_http_client.should_receive(:request_get).with("/test",{"User-Agent"=>"Rapleafbot", "Accept-Encoding"=>"gzip, deflate", "Referer"=>"http://"}).and_return(@test_response)
    @http.get("http://rapleaf.com/test")
  end
  
  it "should raise RobotsTxtViolation if RapleafBot is disallowed on top level path" do
    robots_txt_body = <<-STR
    User-agent: NextGenSearchBot
    Disallow: /

    User-agent: Rapleafbot
    Disallow: /
    STR
    @robots_response.stub!(:body).and_return(robots_txt_body)
    @mock_http_client.stub!(:request_get).and_return(@robots_response)
    @mock_http_client.should_not_receive(:request_get).with("/test",{"User-Agent"=>"Rapleafbot", "Accept-Encoding"=>"gzip, deflate", "Referer"=>"http://"})
    lambda { @http.get("http://rapleaf.com/test") }.should raise_error(RobotsTxtViolation)
  end 
  
  it "should raise RobotsTxtViolation if RapleafBot is disallowed on exact path" do
    robots_txt_body = <<-STR
    User-agent: NextGenSearchBot
    Disallow: /

    User-agent: Rapleafbot
    Disallow: /test
    STR
    @robots_response.stub!(:body).and_return(robots_txt_body)
    @mock_http_client.stub!(:request_get).and_return(@robots_response)
    @mock_http_client.should_not_receive(:request_get).with("/test",{"User-Agent"=>"Rapleafbot", "Accept-Encoding"=>"gzip, deflate", "Referer"=>"http://"})
    lambda { @http.get("http://rapleaf.com/test") }.should raise_error(RobotsTxtViolation)
  end 
  
  it "should raise RobotsTxtViolation if all user-agents are disallowed on top level path" do
    robots_txt_body = <<-STR
    User-agent: *
    Disallow: /
    STR
    @robots_response.stub!(:body).and_return(robots_txt_body)
    @mock_http_client.stub!(:request_get).and_return(@robots_response)
    @mock_http_client.should_not_receive(:request_get).with("/test",{"User-Agent"=>"Rapleafbot", "Accept-Encoding"=>"gzip, deflate", "Referer"=>"http://"})
    lambda { @http.get("http://rapleaf.com/test") }.should raise_error(RobotsTxtViolation)
  end
  
  it "should make request if RapleafBot is allowed on path even though all user-agents are disallowed" do
    robots_txt_body = <<-STR
    User-agent: *
    Disallow: /

    User-agent: Rapleafbot
    Allow: /test
    STR
    @robots_response.stub!(:body).and_return(robots_txt_body)
    @mock_http_client.stub!(:request_get).and_return(@robots_response)
    @mock_http_client.should_receive(:request_get).with("/test",{"User-Agent"=>"Rapleafbot", "Accept-Encoding"=>"gzip, deflate", "Referer"=>"http://"}).and_return(@test_response)
    @http.get("http://rapleaf.com/test")
  end
  
  it "should make request if all user-agents are disallowed on empty path" do
    robots_txt_body = <<-STR
    User-agent: *
    Disallow: 
    STR
    @robots_response.stub!(:body).and_return(robots_txt_body)
    @mock_http_client.stub!(:request_get).and_return(@robots_response)
    @mock_http_client.should_receive(:request_get).with("/test",{"User-Agent"=>"Rapleafbot", "Accept-Encoding"=>"gzip, deflate", "Referer"=>"http://"}).and_return(@test_response)
    @http.get("http://rapleaf.com/test")
  end
  
end