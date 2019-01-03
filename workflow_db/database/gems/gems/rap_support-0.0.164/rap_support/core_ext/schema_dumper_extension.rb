module ActiveRecord
  class SchemaDumper
    # Dumps the specified table.
    def self.dump_table(connection, tbl, stream)
      new(connection).dump_table(tbl, stream)
    end
    
    def dump_table(tbl, stream)
      table(tbl, stream)
    end
  end
end
