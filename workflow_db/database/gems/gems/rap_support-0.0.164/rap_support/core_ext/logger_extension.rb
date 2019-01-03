require 'logger'

if RUBY_VERSION < '2.3.0'
  class Logger

    def skip_compression=(bool)
      @logdev.skip_compression = bool
    end

    def skip_compression?
      @logdev.skip_compression
    end

    def skip_flock=(bool)
      @logdev.skip_flock = bool
    end

    def skip_flock?
      @logdev.skip_flock
    end

    private

    class LogDevice

      attr_accessor :skip_compression, :skip_flock

      def write(message)
        raise RuntimeError.new("Cannot log to nfs without using 'skip_flock'.") if !@skip_flock && @filename =~ /^\/var\/nfs\//

        @mutex.synchronize do
          if @shift_age and @dev.respond_to?(:stat)
            # This is a temporary fix until we figure out how to do rotation with multiple processes
            @dev.close
            @dev = create_logfile(@filename, false)

            # acquire lock so only one process can rotate and/or write to the file at a time
            @dev.flock(File::LOCK_EX) unless @skip_flock
            now = Time.now # use a consistent time throughout execution

            begin
              check_shift_log(now)
            rescue RuntimeError => e
              @dev.close
              @dev = create_logfile(@filename, false)
            rescue
              raise Logger::ShiftingError.new("Shifting failed. #{$!.pretty}")
            end

            @dev.write(message)

            # 'touch' is here to solve a synchronization issue that happens when
            #  check_shift_log executes just before the end of a period and the
            #  write executes just after. This would cause the log not to rotate.
            #commented out for now to get other stuff working
            #`touch -m -d '#{now.to_s :db}' #{@filename}`
            @dev.flock(File::LOCK_UN) unless @skip_flock
          else
            @dev.write(message)
          end
        end
      end

      private

      def create_logfile(filename, include_header = true)
        logdev = open(filename, (File::WRONLY | File::APPEND | File::CREAT))
        logdev.sync = true
        add_log_header(logdev) if include_header
        logdev
      end

      def check_shift_log(now)
        if @shift_age.is_a?(Integer)
          # Note: always returns false if '0'.
          if @filename && (@shift_age > 0) && (@dev.stat.size > @shift_size)
            shift_log_age
          end
        else
          if @dev.stat.mtime <= previous_period_end(now)
            shift_log_period(now)
          end
        end
      end


      def shift_log_age
        (@shift_age-3).downto(0) do |i|
          ["", ".gz"].each do |extension|
            if FileTest.exist?("#{@filename}.#{i}#{extension}")
              File.rename("#{@filename}.#{i}#{extension}", "#{@filename}.#{i+1}#{extension}")
            end
          end
        end

        File.rename("#{@filename}", "#{@filename}.0")
        gzip_file("#{filename}.0") unless @skip_compression
        # the lock is removed when @dev is closed so we do this after the rename
        @dev.close
        @dev = create_logfile(@filename)
        return true
      end

      def shift_log_period(now)
        # Override time string format to allow for rotation every minute
        postfix = previous_period_end(now).strftime(datetime_format_string)
        age_file = "#{@filename}.#{postfix}"

        ["", ".gz"].each do |extension|
          if FileTest.exist?("#{age_file}#{extension}")
            raise RuntimeError.new("'#{ age_file }' already exists.")
          end
        end

        File.rename("#{@filename}", age_file)
        gzip_file(age_file) unless @skip_compression
        # the lock is removed when @dev is closed so we do this after the rename
        @dev.close
        @dev = create_logfile(@filename)
        return true
      end

      def gzip_file(file)
        # gzip in background
        Thread.start(file) do |file|
          `gzip '#{file}'`
        end
      end

      # added 'minutely' and 'hourly' to the default logger
      def previous_period_end(now)
        case @shift_age
    	  when /^minutely$/
    	    Time.mktime(now.year, now.month, now.mday, now.hour, now.min, 59) - 60 # 1.minute
    	  when /^hourly$/
    	    Time.mktime(now.year, now.month, now.mday, now.hour, 59, 59) - 3600 # 1.hour
        when /^daily$/
          eod(now - 1 * SiD)
        when /^weekly$/
          eod(now - ((now.wday + 1) * SiD))
        when /^monthly$/
          eod(now - now.mday * SiD)
        else
          now
        end
      end

      def datetime_format_string
        case @shift_age
    	  when /^minutely$/
    	    "%Y%m%d%H%M"
    	  when /^hourly$/
    	    "%Y%m%d%H"
        when /^daily$/
          "%Y%m%d"
        when /^weekly$/
          "%Y%m%d"
        when /^monthly$/
          "%Y%m"
        else
          "%Y%m%d%H%M"
        end
      end

    end
  end
end
