# Override rails' column definitions, make sure varbinary
# gets set to varbinary, instead of blob

if Rails.version < '4.0.0'
  (Rails.version >= '3.2.0' ?
    ActiveRecord::ConnectionAdapters::Mysql2Adapter::Column :
    ActiveRecord::ConnectionAdapters::Mysql2Column
  ).module_eval do
    alias :old_simplified_type :simplified_type
    def simplified_type(field_type)
      return :varbinary if field_type =~ /varbinary/i
      return :bytes if field_type =~ /binary/i
      old_simplified_type(field_type)
    end
  end
end

# Update MysqlAdapter to set reconnect to true on the Mysql object.
ActiveRecord::ConnectionAdapters::Mysql2Adapter.module_eval do

  alias_method :orig_initialize, :initialize

  # Add varbinary and others (see comments next to each)
  def native_database_types #:nodoc:
    {:primary_key => "int(11) auto_increment PRIMARY KEY",
     :string      => { :name => "varchar", :limit => 255 },
     :text        => { :name => "text" },
     :integer     => { :name => "int", :limit => 11 },
     :float       => { :name => "float" },
     :decimal     => { :name => "decimal" },
     :datetime    => { :name => "datetime" },
     :timestamp   => { :name => "datetime" },
     :time        => { :name => "time" },
     :date        => { :name => "date" },
     :binary      => { :name => "blob" },
     :boolean     => { :name => "tinyint", :limit => 1 },
     :smallint    => { :name => 'smallint' }, # added by manish
     :tinyint     => { :name => 'tinyint' }, # added by manish
     :longbinary  => { :name => 'longblob' }, # added
     :varbinary   => { :name => 'varbinary'}, # added by dayo
     :bytes       => { :name => 'binary' } # added bc rails overrides MySQL BINARY type
    }
  end


  # Acquires all the foreign key constraints for a given table. Returns a hash
  # with the following keys:
  #   :name - Name of the constraint
  #   :fk_column - Column for the constraint
  #   :pk_table - Referenced table (primary key)
  #   :pk_column - Referenced column
  #   :ondelete - One of :none, :restrict, :cascade, or :nullify
  def constraints(table)
    rgc = []
    execute("show create table #{table}").each do |l|
      l.to_s.split(/\n/).each do |line|
        next if line !~ /CONSTRAINT/i
        regex = /CONSTRAINT `([^`]+)` FOREIGN KEY \(`([^`]+)`\) REFERENCES `([^`]+)` \(`([^`]+)`\)(.*)/i
        rg_match = regex.match(line)
        raise "Bad regex for #{line}" if !rg_match || rg_match.size < 3
        st_orig, st_fk_name, st_fk_column, st_pk_table, st_pk_column = *rg_match
        kondelete = :none
        if rg_match.size >= 6
          case rg_match[5]
          when /RESTRICT/i
            kondelete = :restrict
          when /CASCADE/i
            kondelete = :cascade
          when /SET NULL/i
            kondelete = :nullify
          end
        end

        rgc << { :name => st_fk_name, :fk_column => st_fk_column, :pk_table => st_pk_table,
                 :pk_column => st_pk_column, :ondelete => kondelete}
      end
    end

    rgc
  end
end

# Add varbinary to TableDefinitions class in Rails 3.0 Beta 4
class ActiveRecord::ConnectionAdapters::TableDefinition
  def varbinary(*args)                                               # def string(*args)
    options = args.extract_options!                                       #   options = args.extract_options!
    column_names = args                                                   #   column_names = args
                                                                          #
    column_names.each { |name| column(name, 'varbinary', options) }  #   column_names.each { |name| column(name, 'string', options) }
  end                                                                     # end

  def bytes(*args)
    options = args.extract_options!
    column_names = args

    column_names.each { |name| column(name, 'bytes', options) }
  end
end


# Add varbinary to AREL for Rails 3.0 Beta 4
module Arel
  module Sql
    module Attributes
      def self.for(column)
        case column.type
        when :string    then String
        when :text      then String
        when :integer   then Integer
        when :float     then Float
        when :decimal   then Decimal
        when :date      then Time
        when :datetime  then Time
        when :timestamp then Time
        when :time      then Time
        when :binary    then String
        when :boolean   then Boolean
        when :varbinary then String
        else
          raise NotImplementedError, "Column type `#{column.type}` is not currently handled"
        end
      end
    end
  end
end
