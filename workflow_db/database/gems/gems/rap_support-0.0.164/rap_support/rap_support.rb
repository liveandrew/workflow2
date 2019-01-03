unless defined? RAP_SUPPORT
RAP_SUPPORT = true

require File.expand_path(File.dirname(__FILE__)) + '/core_ext/core_ext'
$: << File.expand_path(File.dirname(__FILE__)) + '/lib' rescue nil
Dependencies.load_paths << File.expand_path(File.dirname(__FILE__)) + '/lib' rescue nil
require 'rc4'

class Rap_support
  def self.get_autoload_dirs
    rtrn = Array[ File.expand_path(File.dirname(__FILE__)) ,
                  File.expand_path(File.dirname(__FILE__)) + "/core_ext",
                  File.expand_path(File.dirname(__FILE__)) + "/lib"
                ]
     return rtrn
  end

  def self.get_rake_path
    File.expand_path(File.dirname(__FILE__))
  end


  begin
    require 'thread-dump'
     if ThreadDump::VERSION_SUB >= 3
       ThreadDump::Config.output_format = :html
       ThreadDump::Config.dump_target = :file
       # ThreadDump::Monitor.start(30)
     end
    rescue Exception => e
  end
end
end
