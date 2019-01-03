module ActiveRecord
  # Support dependent => :restrict_with_exception and :restrict_with_error in Rails 3.0.20 and 3.2.22
  # Rails 4 breaks compatibility by removing dependent => :restrict, so we backport the new options to Rails 3.x
  module Reflection
    module ClassMethods
      if Rails.version == '3.0.20'
        def create_reflection(macro, name, options, active_record)
          if options[:dependent] && [:restrict_with_exception, :restrict_with_error].include?(options[:dependent])
            options[:dependent] = :restrict
          end
          case macro
            when :has_many, :belongs_to, :has_one, :has_and_belongs_to_many
              klass = options[:through] ? ThroughReflection : AssociationReflection
              reflection = klass.new(macro, name, options, active_record)
            when :composed_of
              reflection = AggregateReflection.new(macro, name, options, active_record)
          end
          write_inheritable_hash :reflections, name => reflection
          reflection
        end
      elsif Rails.version < '4.0.0'
        def create_reflection_patch_dependent_restrict(macro, name, options, active_record)
          if options[:dependent] && [:restrict_with_exception, :restrict_with_error].include?(options[:dependent])
            options[:dependent] = :restrict
          end
          create_reflection_orig(macro, name, options, active_record)
        end

        alias_method :create_reflection_orig, :create_reflection
        alias_method :create_reflection, :create_reflection_patch_dependent_restrict
      end
    end
  end

  class Base
    class << self

      # Allows code to access other databases within the block passed.
      def with_connection( environment )
        klass = Class.new(self)
        klass_name = "Temp#{Time.now.to_i * rand}".gsub('.', '_')
        Object.const_set klass_name, klass
        klass.establish_connection environment

        yield klass

      ensure
        klass.remove_connection
        Object.send :remove_const, klass_name
      end

      # Acquires a new raw connection for the specified environment.
      def with_raw_connection(environment = nil)
        if environment
          config = configurations[environment]
          adapter = config['adapter']
        else
          # Need to pull the config, we're going to dupe the settings for a new connection
          config = self.connection.instance_eval { @config }
          adapter = config[:adapter]
        end

        raw_conn = self.send("#{adapter}_connection", config).raw_connection

        begin
          yield raw_conn
        ensure
          raw_conn.disconnect rescue nil
        end
      end


      # Declares an enumeration over a column.
      #
      # For example, enum :authorization, { :basic => 0, :admin => 1 }
      #
      # This will get you the ability to do self.authorization = :basic,
      # call self.authorization_basic? and self.authorization_admin?,
      # get integer values for the enum via self.class.authorization_to_i(:admin),
      # get symbols for the enum via self.class.i_to_authorization(1),
      # and check validity via self.class.valid_<enum_name>?(value)
      #
      # Accessing the attribute will still give you the integer value and you
      # can still set it via the integer value too.
      #
      # To perform an operation when the enum is updated,
      # instead of defining the = operator, define <enum_name>_updated
      #
      # You will not be allowed to set a value not explicit in the enum.
      def enum(target, domain)
        class_eval {
          @enum_targets ||= []
          @enum_targets << target
        }

        meta_def "#{target.to_s.pluralize}" do
          domain
        end

        meta_def "i_to_#{target.to_s}" do |val|
          ret = nil
          domain.each do |k, v|
            ret = k if v == val
          end
          ret
        end

        meta_def "#{target.to_s}_to_i" do |val|
          if val.kind_of? Fixnum then val
          else
            raise "Invalid value for " + self.to_s + " " + target.to_s + ": " + val.to_s + " should be " +
            domain.keys.to_sentence(:connector => "or") unless domain[val]
            domain[val]
          end
        end

        meta_def "valid_#{target.to_s}?" do |val|
          if val.kind_of? Fixnum then domain.has_value?(val)
          else domain.has_key?(val)
          end
        end

        domain.each do |enum_sym, enum_value|
          class_def "#{target.to_s}_#{enum_sym.to_s}?" do
            self.send("#{target.to_s}").eql?(enum_value)
          end
        end

        class_def "#{target.to_s}=" do |val|
          if val.nil?
            super_val = val;
          elsif val.kind_of? Fixnum
            super_val = val if domain.find { |enum_key, enum_val| val.eql?(enum_val) }
          else
            super_val = domain[val]
          end

          raise "Invalid value for " + target.to_s + ": " + val.to_s unless super_val || val.nil?

          update_method = "before_" + target.to_s + "_update"
          self.send(update_method, val) if self.respond_to? update_method

          super super_val
        end

        meta_def "method_missing" do |method, *args|
          return super(method.to_s, *args) unless (method.to_s.match(/^find_(or_create_|all_)?by_/) && @enum_targets.detect{|t| method.to_s.include? t.to_s})
          col_names = method.to_s.gsub(/find_(or_create_|all_)?by_/,'').split('_and_')
          new_args = col_names.zip(args).map do |col, arg|
            targ = @enum_targets.detect{|t| col==t.to_s and self.send("valid_#{t.to_s}?", arg)}
            targ.nil? ? arg : self.send("#{targ.to_s}_to_i", arg)
          end
          super(method.to_s, *new_args)
        end

      end

      # Makes it easy to declare an enum based on a thrift enum
      def enum_from_thrift(target, thrift_enum)
        enum(target, Hash[thrift_enum::VALUE_MAP.map{|id, name| [name.downcase.to_sym, id]}])
      end

      # Allows caller to send in a SQL snippet or array of SQL snippets that
      # will be joined together to form a conditions clause.
      #
      # Args
      # conditions - (String or Array of Strings), e.g. "username = 'jlizt'" or
      #              ["score != 1", "relationship = 2"]
      # how_many - (Symbol) :first or :all (Optional, defaults to :first)
      def find_with_conditions(conditions = [], how_many = :first)
        if(conditions.blank?) then find(how_many)
        else find(how_many, :conditions => conditions.to_a.join(" AND ")); end
      end
    end


    # Write a fixture file for testing
    def self.to_fixture
      hsh = self.find(:all).inject({}) { |hsh, record| hsh.merge(record.id => record.attributes) }
      File.open("#{RAILS_ROOT}/test/fixtures/#{table_name}.yml", 'w+') do |f|
        f.puts hsh.to_yaml
      end
    end

    # Return the yaml for a specific record
    def to_fixture
      {"record_#{self.id}" => self.attributes}.to_yaml
    end

  end

end
