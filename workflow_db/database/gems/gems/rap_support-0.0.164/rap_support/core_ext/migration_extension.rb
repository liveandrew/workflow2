# With all migrations (or at least most), come indexes, and the only way
# to add indexes to new tables is with the add_index method. Personally,
# I have always found this to be a pain, and somewhat unnecessary. So,
# today, I thought I would dive into the Rails source and see if I could
# make this a little better.
#
# Old Migration
#   def self.up
#     create_table :songs do |t|
#       t.column :name, :string
#       t.column :length, :integer
#       t.column :album_id, :integer
#       t.column :artist_id, :integer
#     end
#
#     add_index :songs, :album_id
#     add_index :songs, :artist_id
#     add_index :songs, :length, :name => 'len_index'
#     add_index :songs, [:name, :album_id], :unique => true
#   end
#
# New Migration
#   def self.up
#     create_table :songs do |t|
#       t.column :name, :string
#       t.column :length, :integer
#       t.column :album_id, :integer
#       t.column :artist_id, :integer
#
#       t.index :album_id
#       t.index :artist_id
#       t.index :length, :name => 'len_index'
#       t.index [:name, :album_id], :unique => true
#     end
#   end
#
module ActiveRecord
  module ConnectionAdapters
    class TableDefinition
      attr_accessor :rap_support_migration_extension_indexes

      # TableDefinition#index adds an index to be created
      # to a list of indexes that will be created after
      # the table is created.
      def index(columns, options = {})
        # $table_definition_self holds a reference to the
        # TableDefinition instance that is passed to the
        # block of the create_table method. Getting a
        # handle on this TableDefinition allows access to
        # the indexes after the table has been created.
        $table_definition_self = self

        @rap_support_migration_extension_indexes ||= []
        index = IndexDefinition.new
        index.columns = [*columns]
        index.unique  = options[:unique]
        index.name    = options[:name] || (index.columns.join('_')[0..57] + '_index')
        @rap_support_migration_extension_indexes << index

        self
      end
    end

    module SchemaStatements
      # Using a unique name for the old create_table
      # method rename just in case someone else is
      # using the standard name 'old_create_table'.
      OLD_CREATE_TABLE = ("create_table_" + Time.now.to_i.to_s).to_sym
      alias_method OLD_CREATE_TABLE, :create_table

      # create_table is being modified to create do the
      # standard table creation first Then if the
      # $table_definition_self variable was set, and
      # indexes were added, we will call the add_index
      # method for every index added through the new
      # interface.
      def create_table(name, *args, &block)
        $table_definition_self = nil

        # Create the table the standard way.
        send(OLD_CREATE_TABLE, name, *args, &block)

        # Add all of the indexes defined on the table
        # secondly.
        if $table_definition_self
          $table_definition_self.rap_support_migration_extension_indexes.each do |index|
            add_index(
              name,
              index.columns,
              :unique => index.unique,
              :name => index.name
            )
          end
        end
      end
    end
  end
end
