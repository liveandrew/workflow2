require File.dirname(__FILE__) + '/schema_checker'

class IndexChecker < SchemaChecker
  
  COLUMNS_TO_IGNORE_INDEXES_FOR = "columns_to_ignore_indexes_for.yml"
  
  def check_all_tables
    table_errors = {}
    get_tables.each do |tbl|
      if (column_errors = check_table(tbl)).any?
        table_errors[tbl] = column_errors
      end
    end
    table_errors
  end
  
  def ignore_table_errors(table_errors)
    current_ignores = get_columns_to_ignore_indexes_for
    table_errors.each do |tbl, column_errors|
      current_ignores[tbl] = (current_ignores[tbl]||{}).merge(column_errors)
    end
    set_columns_to_ignore_indexes_for(current_ignores)
  end
  
  def check_table(tbl)
    column_errors = {}
    
    klass = klass_from_table_name(tbl) rescue nil
    return column_errors if klass.nil?
    
    columns_to_ignore = get_columns_to_ignore_indexes_for[tbl] || {}
    @connection.columns(tbl).each do |col|
      next if columns_to_ignore[col.name]
      
      belongs_to_assoc = get_belongs_to_assoc(klass, col.name)
      is_id_column = col.name =~ /_id$/
      next if !is_id_column && !belongs_to_assoc
      
      if !part_of_an_index?(tbl, col.name)
        column_errors[col.name] = :not_indexed
      elsif !first_column_in_index?(tbl, col.name)
        column_errors[col.name] = :not_the_first_column_in_any_index
      end
    end
    
    column_errors
  end
  
  private
  
  def get_columns_to_ignore_indexes_for
    @columns_to_ignore_indexes_for ||= YAML.load_file(BASE_DIR + COLUMNS_TO_IGNORE_INDEXES_FOR)
    @columns_to_ignore_indexes_for
  end
  
  def set_columns_to_ignore_indexes_for(columns_to_ignore_indexes_for)
    File.open(BASE_DIR + COLUMNS_TO_IGNORE_INDEXES_FOR, 'w'){|f| f.write(YAML.dump(columns_to_ignore_indexes_for))}
    @columns_to_ignore_indexes_for = columns_to_ignore_indexes_for
  end
  
  def first_column_in_index?(tbl, col_name)
    table_indexes(tbl).each do |index_definition|
      if index_definition.columns[0] == col_name
        return true
      end
    end
    false
  end
  
  def part_of_an_index?(tbl, col_name)
    table_indexes(tbl).each do |index_definition|
      if index_definition.columns.include?(col_name)
        return true
      end
    end
    false
  end
  
  def table_indexes(tbl)
    @indexes ||= {}
    @indexes[tbl] ||= @connection.indexes(tbl)
  end
  
end
