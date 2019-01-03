# Ideas taken from http://github.com/fizx/robots/
# Enables simple allow/disallow abiding of robots.txt
class RobotsTxtViolation < Exception; end

class RobotsTxt

  def initialize
    @parsed_rules_by_host = {}
  end
  
  
  def has_rules_for_host?(host)
    !!@parsed_rules_by_host[host]
  end
  
  
  # parse and store rules for this host
  def parse_robots_txt(robots_txt_body, host)
    allows = {}; disallows = {}; agent = nil
    robots_txt_body.each_line do |line|
      next if line =~ /^\s*$|^\s*#.*$/ # ignore comments and blank lines
      key, value = line.split(":", 2).map{|i| i.strip}
      next if value.blank?

      case key
      when "User-agent"
        agent = value

      when "Allow"
        allows[agent] ||= []
        allows[agent] << value

      when "Disallow"
        disallows[agent] ||= []
        disallows[agent] << value

      else
        # don't worry about crawl delay or other rules for now
      end
    end
    
    @parsed_rules_by_host[host] = {:allows => allows, :disallows => disallows}
  end
  

  def allows?(uri, user_agent)
    allowed = true
    request_path = uri.request_uri
    rules = @parsed_rules_by_host[uri.host] || {}
    disallows = rules[:disallows] || {}
    allows = rules[:allows] || {}

    # handle disallows first
    disallows.each do |agent, paths|
      allowed = false if (/^\*$|#{user_agent}/ =~ agent) and (paths.detect{|path| request_path.starts_with?(path)})
    end
    
    allows.each do |agent, paths|
      allowed = true if paths.detect{|path| (/^\*$|#{user_agent}/ =~ agent) and (request_path == path) }
    end

    allowed
  end
  
end
