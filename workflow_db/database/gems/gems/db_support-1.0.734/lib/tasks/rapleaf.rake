# DatabaseCreator (re)creates databases for the given ActiveRecord database
# config.
class DatabaseCreator
  def self.mysql_command(config, database_name)
    command = [
      'mysql', '--user=root',
      "--host=#{config.fetch('host')}",
      "--execute=CREATE DATABASE #{database_name}"
    ]
    command << "--port=#{config['port']}" if config['port']
    command << "--socket=#{config['socket']}" if config['socket']
    command
  end

  def self.exec_create(config, database_name)
    command = mysql_command(config, database_name)
    IO.popen(command) do |io|
      while (line = io.gets)
        print "MYSQL: #{line}"
      end
    end
  end

  def self.create(config, database_name)
    puts "Creating #{database_name} for the first time..."
    exec_create(config, database_name)
  end

  def self.recreate(env_name, config, database_name)
    puts "Recreating database for #{env_name}: #{database_name}..."
    begin
      ActiveRecord::Base.connection.recreate_database(database_name)
    rescue Mysql2::Error
      create(config, database_name)
    end
  end
end

namespace :db do

  desc "Runs rapleaf:migrate"
  task :migrate do
    Rake::Task["rapleaf:migrate"].invoke
  end

  desc "Prepares database.yml if necessary"
  Rake::Task["db:load_config"].enhance ["rapleaf:prepare_local_database"]

end

namespace :rapleaf do
  # if this project uses the database.local pattern then it may not have a database.yml
  # (since that project shouldn't come with one) so we create a database.yml file using
  # the contents of database.local
  task :prepare_local_database do
    if Rails.env.development?
      if File.exist?('config/database.local') && !File.exist?('config/database.yml')
        heading "Copying database.local to database.yml"
        FileUtils.cp('config/database.local', 'config/database.yml')
      end
    end
  end

  task :triggers => :environment do
    f = "db/triggers.sql"
    if File.exist?(f)
      triggers = File.read(f).gsub("\n", "").split(';').reject { |t| t == "" }
      triggers.each { |t| ActiveRecord::Base.connection.execute(t) }
    end
  end

  task :recreate_dbs => :environment do

    dbs = {}
    ActiveRecord::Base.configurations.each do |name, config|
      if name =~ /#{Rails.env}/
        db = config['database']
        # Special case for espdb
        db = config['esp_partition_0'] && config['esp_partition_0']['database'] unless db
        if db
          if dbs[db]
            puts "Already recreated database for #{name}: #{db}..."
          else
            dbs[db] = 1
            DatabaseCreator.recreate(name, config, db)
          end
        end
      end
    end
    ActiveRecord::Base.establish_connection(Rails.application.config.database_configuration[Rails.env])
  end

  def heading(str)
    puts "*** #{str}"
  end

  desc "Similar to db:migrate, but does special rapleaf stuff (like pull migrations from the rldb gem)"
  task :migrate => :environment do

    if Rails.env.development?
      hostname = Rails.configuration.database_configuration[Rails.env]['host']
      if hostname != '127.0.0.1' && hostname != 'localhost'
        raise "Misconfigured database.yml. Hostname is #{hostname} but should either be localhost or 127.0.0.1. "\
        "Perhaps you're trying to run migrations against dev-db."
      end
    end


    if ARGV.include?("--recreate") || ARGV.include?("rapleaf:init_db")
      heading "Recreating database"
      Rake::Task["rapleaf:recreate_dbs"].invoke
    end

    version = ENV["VERSION"] ? ENV["VERSION"].to_i : nil

    migration_dirs = [DbSupport::DB_PATH + "/migrate"]
    trigger_files = [DbSupport::DB_PATH + "/triggers/triggers.sql"]

    # Run migrations
    migration_dirs.each do |dir|
      heading "Running Migrations In #{dir}"
      ActiveRecord::Migrator.migrate(dir, version)
    end

    # Load triggers
    trigger_files.each do |file|
      heading "Loading Triggers From #{file}"
      triggers = File.read(file).gsub("\n", "").split(';').reject { |t| t == "" }
      triggers.each { |t| ActiveRecord::Base.connection.execute(t) }
    end

    ## Dump schema
    if File.exists?("db/schema.rb") && ActiveRecord::Base.schema_format == :ruby
      heading "Dumping Schema"
      Rake::Task["db:schema:dump"].invoke
    end

    # Load fixtures
    if !Rails.env.production? && !Rails.env.ops?
      heading "Loading Fixtures"
      Rake::Task['db:fixtures:load'].invoke
    end

    # Check that schema looks good
    if !Rails.env.production? && !Rails.env.ops? && File.directory?('db')
      heading "Checking Schema"
      Rake::Task["rapleaf:check_schema"].invoke
    end

    heading "Done"
  end

  desc "Check for missing indexes"
  task :check_indexes => :environment do

    require File.expand_path('../../schema_checker/index_checker', __FILE__)
    ic = IndexChecker.new
    table_errors = ic.check_all_tables
    if !table_errors.blank?
      heading "ERROR: MISSING INDEX(ES)"
      table_errors.sort.each do |tbl, column_errors|
        puts "#{tbl}:"
        column_errors.each do |col, reason|
          puts "\t#{col} is #{reason.to_s}"
        end
      end

      raise "If you think about it and decide you do not want indexes for these columns, run 'rake rapleaf:ignore_missing_indexes' to ignore them."
    end
  end

  desc "Check for missing associations"
  task :check_associations => :environment do
    require File.expand_path('../../schema_checker/association_checker', __FILE__)
    ac = AssociationChecker.new
    table_errors = ac.check_all_tables
    if !table_errors.blank?
      heading "ERROR: MISSING ASSOCIATION(S)"
      table_errors.sort.each do |tbl, column_errors|
        puts "#{tbl}:"
        column_errors.each do |col, reason|
          puts "\t#{col} is #{reason.to_s}"
        end
      end

      raise "If you think about it and decide you do not want associations for these columns, run 'rake rapleaf:ignore_missing_associations' to ignore them."
    end
  end

  desc "Set all missing indexes to be ignored"
  task :ignore_missing_indexes => :environment do
    require File.expand_path('../../schema_checker/index_checker', __FILE__)
    ic = IndexChecker.new
    ic.ignore_table_errors(ic.check_all_tables)
    puts "Done"
  end

  desc "Set all missing associations to be ignored"
  task :ignore_missing_associations => :environment do
    require File.expand_path('../../schema_checker/association_checker', __FILE__)
    ac = AssociationChecker.new
    ac.ignore_table_errors(ac.check_all_tables)
    puts "Done"
  end

  desc "Add any tables without models to the ignore list"
  task :ignore_missing_models => :environment do
    require File.expand_path('../../schema_checker/schema_checker', __FILE__)
    sc = SchemaChecker.new
    sc.set_tables_to_ignore(sc.get_tables_to_ignore + sc.get_missing_models)
    puts "Done"
  end

  desc "Similar to db:migrate, but also runs migrations in include/db/*"
  task :init_db => :environment do
    Rake::Task["rapleaf:migrate"].invoke
  end

  task :spec => 'rapleaf:init_db' do
    Rake::Task['spec'].invoke
  end

  desc "Check the database to see what the current version of the database is for the current environment."
  task :check_schema => :environment do
    require File.expand_path('../../schema_checker/schema_checker', __FILE__)
    sc = SchemaChecker.new
    if (tables = sc.get_missing_models).any?
      tables.each do |tbl|
        heading "No model found for #{tbl}. This probably means we removed the model and forgot to write a migration to drop the table. It could also mean there is a syntax error in the model preventing it from loading."
      end
      raise "#{tables.size} tables do not have corresponding models. If you do not want models for all of these tables you can run 'rake rapleaf:ignore_missing_models' to ignore them."
    end

    Rake::Task["rapleaf:check_indexes"].invoke
    Rake::Task["rapleaf:check_associations"].invoke
  end
end
