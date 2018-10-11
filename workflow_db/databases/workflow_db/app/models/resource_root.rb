class ResourceRoot < ActiveRecord::Base
  has_one :workflow_execution_resource_root, :dependent => :destroy
  has_many :resource_records, :dependent => :destroy
end
