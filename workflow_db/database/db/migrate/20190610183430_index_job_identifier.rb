class IndexJobIdentifier < ActiveRecord::Migration
  def self.up
    add_index :mapreduce_jobs, :job_identifier
  end

  def self.down
    remove_index :mapreduce_jobs, :job_identifier
  end
end
