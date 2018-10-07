class IncreasedStepDependencySize < ActiveRecord::Migration
  def self.up
    execute "SET sql_log_bin=0"
    execute "ALTER TABLE step_dependencies MODIFY COLUMN id BIGINT AUTO_INCREMENT"
  end

  def self.down
    execute "SET sql_log_bin=0"
    execute "ALTER TABLE step_dependencies MODIFY COLUMN id INTEGER AUTO_INCREMENT"
  end
end
