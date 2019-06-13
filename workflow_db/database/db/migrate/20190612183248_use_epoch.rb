class UseEpoch < ActiveRecord::Migration
  def change
    add_column :workflow_attempts, :last_heartbeat_epoch, :integer, limit: 8
    add_column :background_workflow_executor_infos, :last_heartbeat_epoch, :integer, limit: 8
  end
end
