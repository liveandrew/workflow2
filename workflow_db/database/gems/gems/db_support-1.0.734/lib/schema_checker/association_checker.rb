require File.dirname(__FILE__) + '/schema_checker'

class AssociationChecker < SchemaChecker

  COLUMNS_TO_IGNORE_ASSOCIATIONS_FOR = "columns_to_ignore_associations_for.yml"

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
    current_ignores = get_columns_to_ignore_associations_for
    table_errors.each do |tbl, column_errors|
      current_ignores[tbl] = (current_ignores[tbl]||{}).merge(column_errors)
    end
    set_columns_to_ignore_associations_for(current_ignores)
  end

  def check_table(tbl)
    column_errors = {}

    klass = klass_from_table_name(tbl) rescue nil
    return column_errors if klass.nil?

    columns_to_ignore = get_columns_to_ignore_associations_for[tbl] || {}
    @connection.columns(tbl).each do |col|
      next if columns_to_ignore[col.name]

      belongs_to_assoc = get_belongs_to_assoc(klass, col.name)
      is_id_column = col.name =~ /_id$/
      next if !is_id_column && !belongs_to_assoc

      if belongs_to_assoc.nil?
        column_errors[col.name] = 'missing belongs_to association'
      else
        other_klass = belongs_to_assoc.klass
        other_assoc = assoc_for_column(other_klass, (Rails.version >= '3.1.0' ? belongs_to_assoc.foreign_key : belongs_to_assoc.primary_key_name), klass)
        if other_assoc.nil?
          column_errors[col.name] = "missing a has_many/has_one relationship from #{belongs_to_assoc.class_name}"
        else
          dependent = other_assoc.options && other_assoc.options[:dependent]
          if dependent.nil?
            column_errors[col.name] = "missing a :dependent specifier on the #{other_assoc.macro.to_s} relationship #{other_assoc.name} on #{belongs_to_assoc.class_name}"
          end
        end
      end
    end

    column_errors
  end

  private

  def get_columns_to_ignore_associations_for
    @columns_to_ignore_associations_for ||= YAML.load_file(BASE_DIR + COLUMNS_TO_IGNORE_ASSOCIATIONS_FOR)
    @columns_to_ignore_associations_for
  end

  def set_columns_to_ignore_associations_for(columns_to_ignore_associations_for)
    File.open(BASE_DIR + COLUMNS_TO_IGNORE_ASSOCIATIONS_FOR, 'w'){|f| f.write(YAML.dump(columns_to_ignore_associations_for))}
    @columns_to_ignore_associations_for = columns_to_ignore_associations_for
  end

  # Looks up an association in the given class based upon the given column
  def assoc_for_column(klass, col_name, klass_belongs_to = nil)
    return unless (klass.methods.include?("reflections") || klass.methods.include?(:reflections))

    #puts "Looking for #{!klass_belongs_to ? 'belongs to' : 'has many or one'} association with col_name #{col_name} on #{klass} with #{klass.reflections.size}"

    klass.reflections.each_value do |asc|
      #puts asc.inspect
      next if (Rails.version >= '3.1.0' ? asc.foreign_key : asc.primary_key_name).to_s != col_name.to_s

      if klass_belongs_to
        #puts "#{asc.macro} should be many or one"
        next if (asc.macro != :has_many && asc.macro != :has_one)
        #puts "#{klass_belongs_to.name} should match #{asc.class_name}"
        next if asc.class_name != klass_belongs_to.name.gsub(/.*::/, "")
      else
        #puts "#{asc.macro} should be belongs to"
        next if asc.macro != :belongs_to
      end

      return asc
    end

    nil
  end

end
