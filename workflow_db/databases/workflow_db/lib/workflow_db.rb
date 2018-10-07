require 'workflow_db/paths'

require 'rap_support/core_ext/active_record_extension' # Support for 'enum'
require 'db_support'

module DbSupport
  DB_PATH=File.dirname(__FILE__) + '/../db/'
end

module WorkflowDb
  class Engine < ::Rails::Engine
    def initialize
      # Add shared ruby code to autoload paths so they get recognized by the parent application
      PATHS.each { |path| config.autoload_paths << File.expand_path(File.join('../..', path), __FILE__) }
    end

    rake_tasks do
      gem_root = Gem.loaded_specs['db_support'].full_gem_path
      load "#{gem_root}/lib/tasks/rapleaf.rake"
      load "#{gem_root}/lib/tasks/cruise.rake"
    end
  end
end
