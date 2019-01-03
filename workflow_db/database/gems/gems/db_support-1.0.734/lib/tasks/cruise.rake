task :cruise do
  Rake::Task['cruise:remote'].invoke
end

namespace :cruise do
  task :set_hudson_configs do 
    sh "cp config/database.hudson_tpl config/database.yml"
  end

  task :set_test_env do
    ENV['RAILS_ENV'] = 'test'
    RAILS_ENV = 'test'
  end

  task :rspec_test => [:set_test_env, 'rapleaf:init_db'] do
    sh "bundle exec rspec spec/all.rb"
  end

  task :local => [:set_test_env, :all]
  
  task :remote => :set_test_env do
    ENV["remote"] = "remote"
    Rake::Task['cruise:all'].invoke
  end
  
  task :all => ['rapleaf:init_db', 'spec', 'test:units', 'test:functionals', 'test:integration']

  task :rcov do
    File.delete(Dir.pwd + "/coverage.data") if File.exists?(Dir.pwd + "/coverage.data")

    # rcov is a code coverage tool for ruby. It will run ruby scripts,
    # track code coverage, and output to an html file. rcov has been
    # integrated into our cruise task and can be enabled by commenting/uncommenting
    # the lines in the ruby() override below in module RakeFileUtils. The
    # rcov html output is linked from the cruise webpage.
    module RcovTestSettings
      class << self
        attr_accessor :aggregate, :enabled
        def to_params
          if self.aggregate
            "-t --rails --no-html --aggregate coverage.data"
          else
            "--rails --aggregate coverage.data"
          end
        end
      end
    end

    module RakeFileUtils
      alias :ruby_without_rcov :ruby

      def ruby(*args, &block)
        #if RcovTestSettings.enabled
        if false
          cmd = "rcov #{RcovTestSettings.to_params} #{args}"
          return sh(cmd, {}, &block)
        else
          ruby_without_rcov(*args, &block)
        end
      end
    end
    
    RcovTestSettings.aggregate, RcovTestSettings.enabled = false, false
    Rake::Task['cruise:all'].invoke
  end

end
