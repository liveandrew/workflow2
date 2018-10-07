class Workflow::WorkflowExecution < Workflow::BaseModel
  belongs_to :application
  has_many :workflow_attempt, :dependent => :restrict_with_exception
  has_many :workflow_execution_configured_notification, :dependent => :destroy
  has_many :workflow_alert_workflow_execution, :dependent => :destroy
  has_one :workflow_execution_resource_root, :dependent => :destroy

  enum_from_thrift :app_types, Liveramp::Importer::AppType
  enum_from_thrift :status, Liveramp::Types::Workflow::WorkflowExecutionStatus
end
