require 'yaml'

class SchemaChecker

  TABLES_TO_IGNORE = "tables_to_ignore.yml"
  BASE_DIR = Rake.application.original_dir + "/lib/schema_checker/"

  CUSTOM_CLASS_NAMES = {
    'connector_credentials' => :ConnectorCredentials
  }

  def initialize
    @connection = ActiveRecord::Base.connection

    # Make sure all models are loaded
    Rails::Application.descendants.each do |descendant|
      begin
        descendant.eager_load!
      rescue StandardError => e
        puts "!!!! WARNING !!!! -- Error eager loading models.  This may not be an error if this is a Rails 4 app.  Error: #{e}"
        # In Rails 4+, the eager_load! call does not work for all Rails::Application.descendants
      end
    end

    @table_models = Hash[
      ActiveRecord::Base.descendants.
        select { |x| x.respond_to?(:table_name) && x.table_name }.
        reject { |x| x.abstract_class? }.
        map do |x|
          # Skip STI tables
          if x.superclass == ActiveRecord::Base || x.superclass.abstract_class?
            # When this method is called in a downstream rails app, it seems
            # like calling x.table_name does not return the correct result when
            # the table has a base model that is not ActiveRecord::Base. This
            # forces rails to re-generate the table name.  This is awfully hacky,
            # but probably better than re-implementing table name guessing that
            # rails already does.
            table_name = x.reset_table_name
            [table_name, x]
          end
        end.
        reject(&:nil?)
    ]
  end

  def get_tables
    ActiveRecord::Base.connection.tables - get_tables_to_ignore
  end

  def get_missing_models
    get_tables.select do |tbl|
      klass = klass_from_table_name(tbl) rescue nil
      klass.nil?
    end
  end

  def get_tables_to_ignore
    @tables_to_ignore ||= YAML.load_file(BASE_DIR + TABLES_TO_IGNORE)
  end

  def set_tables_to_ignore(tables_to_ignore)
    File.open(BASE_DIR + TABLES_TO_IGNORE, 'w'){|f| f.write(YAML.dump(tables_to_ignore))}
    @tables_to_ignore = tables_to_ignore
  end

  # Gets a Class for the given klass_name
  def klass_from_table_name(tbl)
    return @table_models[tbl] if @table_models[tbl]

    sym = CUSTOM_CLASS_NAMES[tbl]
    sym ||= tbl.camelize.singularize.to_sym
    Class.const_get(sym)
  end

  def get_belongs_to_assoc(klass, col_name)
    klass.reflections.each_value do |reflection|
      if reflection.macro == :belongs_to && (Rails.version >= '3.1.0' ? reflection.foreign_key : reflection.primary_key_name).to_s == col_name.to_s
        return reflection
      end
    end
    nil
  end

end
