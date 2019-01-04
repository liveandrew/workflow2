# Load the rails application
require File.expand_path('../application', __FILE__)

gem 'activerecord'

require 'thrift'
require "rapleaf_types/enums_constants"
require "rapleaf_types/new_person_data_constants"
require "rapleaf_types/liveramp_importer_types"
require 'workflow_constants'

# Initialize the rails application
WorkflowDb::Application.initialize!


require File.expand_path('../../lib/patches/57key.rb', __FILE__)
