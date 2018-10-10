# Load the rails application
require File.expand_path('../application', __FILE__)

gem 'activerecord'

require 'thrift'
require 'rap_support'
require 'rap_support/core_ext/active_record_extension' # Support for 'enum' and 'enum_from_thrift'

# Initialize the rails application
WorkflowDb::Application.initialize!


require File.expand_path('../../lib/patches/57key.rb', __FILE__)
