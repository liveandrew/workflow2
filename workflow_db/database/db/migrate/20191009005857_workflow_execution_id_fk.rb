class WorkflowExecutionIdFk < ActiveRecord::Migration
  def change
    change_column :execution_tags, :workflow_execution_id, :integer, limit: 4
    change_column :workflow_execution_configured_notifications, :workflow_execution_id, :integer, limit: 4
    change_column :workflow_alert_workflow_executions, :workflow_execution_id, :integer, limit: 4

    add_foreign_key :workflow_attempts, :workflow_executions, on_delete: :cascade
    add_foreign_key :execution_tags, :workflow_executions, on_delete: :cascade
    add_foreign_key :workflow_execution_configured_notifications, :workflow_executions, on_delete: :cascade
    add_foreign_key :workflow_alert_workflow_executions, :workflow_executions, on_delete: :cascade
  end
end
