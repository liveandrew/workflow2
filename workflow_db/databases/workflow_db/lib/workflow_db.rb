require 'workflow_db/paths'

require 'rap_support/core_ext/active_record_extension' # Support for 'enum'

module DbSupport
  DB_PATH=File.dirname(__FILE__) + '/../db/'
end

module WorkflowDb
  class Engine < ::Rails::Engine
    def initialize
      # Add shared ruby code to autoload paths so they get recognized by the parent application
      PATHS.each { |path| config.autoload_paths << File.expand_path(File.join('../..', path), __FILE__) }
    end
  end
end
