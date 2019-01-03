require 'capistrano'
require 'stomp'

module DeployCommon

  # look at the cluster-exclude.txt file and remove hosts listed in
  # it from the roles. note this works in place.  it's a simple file
  # format -- comments (#) are removed and then the file is split on
  # spaces.  all remaining tokens are read as hostnames.
  def clean_roles!(exclude_file='/apps/deploy/deploy-exclude.txt')
    excludes = File.open(exclude_file) do |f|
      f.read().gsub(/\s*#.*/, '').split(/\s+/).select {|_| _.size > 0}.sort
    end rescue []

    if excludes.size > 0
      puts "Excluding hosts: #{excludes.join(", ")}"

      roles.each_pair do |k,v|
        roles[k] = v.select {|_| not excludes.include?(_.host)}
      end
    end
  end

  def is_numeric?(s)
    begin
      Float(s)
    rescue
      false # not numeric
    else
      true # numeric
    end
  end
  

  def get_deployer
    ENV['user'] || ENV['USER'] || ""
  end

  def publish_message(txt)
    headers = { :persistent => true, :project => project, :cmd => "cap #{cmd_args}" }
    headers['user'] = user_message unless user_message.empty?
    if ENV['rev']
      headers['rev'] = ENV['rev']
      headers['url' ]= is_numeric?(ENV['rev']) ? "https://code.liveramp.net/changeset/#{ENV['rev']}" : "https://git.liveramp.net/MasterRepos/#{project}/commit/#{ENV['rev']}"
    end

    f_creds = File.open("/apps/deploy/figg/figg_private/msgbrokers/production/amq_broker_creds.txt", "r")
    username, password = f_creds.readlines.first.split(':')
    f_creds.close
    password.chop!
    
    stomp_hosts = {
      :hosts => [ {:login => username, :passcode => password, :host => "activemq01", :port => 6163},
                  {:login => username, :passcode => password, :host => "activemq02", :port => 6163} ],
      :randomize => false, :max_reconnect_attempts => 0 }
    begin
      client = Stomp::Client.new(stomp_hosts)
      client.publish "/topic/deploy", txt, headers
      client.close
    rescue Stomp::Error
      puts "WARNING: Could not publish to ActiveMQ."
    end
  end

  def configure(configuration)
    set_environment_variables(configuration)
    set_trigger_actions(configuration) if configuration.support_triggers
    set_roles(configuration)
  end

  def notify_start
    sleepy_time = ENV['sleep'].to_i || 30
    set :user_message, get_deployer()
    set :project, `pwd`.chomp.split("/").last
    set :cmd_args, "#{$*.join(" ")}"

    unless ENV['quiet']
      publish_message("deploying in #{sleepy_time} secs")
    end

    puts "  * sleeping #{sleepy_time} secs..."
    sleep sleepy_time
    set :started_at, Time.now
  end

  def notify_failed
    set :failed_at, Time.now
    run_time = (failed_at - started_at).round

    unless ENV['quiet']
      if exists?(:webapp_notify)
        publish_message("deploy failed on #{webapp_notify} #{run_time / 60}m #{run_time % 60}s")
      else
        publish_message("deploy failed in #{run_time / 60}m #{run_time % 60}s")
      end
    end

    unless ENV['quiet']
      puts "Started at #{started_at}"
      puts "Failed at #{failed_at}"
      puts "Total Elapsed Time: #{run_time / 60}m #{run_time % 60}s"
    end
  end

  def notify_finish
    set :finished_at, Time.now
    run_time = (finished_at - started_at).round

    unless ENV['quiet']
      if exists?(:webapp_notify)
        publish_message("deploy on #{webapp_notify} #{run_time / 60}m #{run_time % 60}s")
      else
        publish_message("deploy complete in #{run_time / 60}m #{run_time % 60}s")
      end
    end

    unless ENV['quiet']
      puts "Started at #{started_at}"
      puts "Finished at #{finished_at}"
      puts "Total Elapsed Time: #{run_time / 60}m #{run_time % 60}s"
    end
  end

  private
  def set_environment_variables(configuration)
    configuration.config.each_pair do |k, v|
      set k, v if v != nil
    end
  end

  def set_trigger_actions(c)
   c.trigger_actions.each_pair do |trigger, actions|
      actions.each do |entry|
        except = entry[1]
        if except
          on trigger, entry[0], :except => except
        else
          on trigger, entry[0]
        end
      end
    end
  end

  def set_roles(configuration)
    configuration.role.each_pair do |k, v|
      if v != nil
        if v.class == Array
          role(k, *(v))
        else
          role(k, v)
        end
      end
    end
  end

end

Capistrano.plugin :deploy_common, DeployCommon

