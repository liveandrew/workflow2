require 'capistrano'
require 'json'
require 'net/https'
require 'uri'
require 'date'

module CheckVersion
  OAUTH_TOKEN = "3c1275cee9540475777d8a289884b51a323d44f0"
  API_ROOT = "https://git.liveramp.net/api/v3"

  def get_revisions
    revs = Hash.new { |h, k| h[k] = "" }
    rev_path = "#{deploy_to}/current/REVISION"
    run "test -f #{rev_path} && cat #{rev_path} || echo 0" do |ch, stream, data|
      revs[ch[:host]] << data
    end

    return revs
  end

  def get_datetime_for_revision(git_revision, organization)
    headers = { "Authorization" => "token #{OAUTH_TOKEN}" }

    #Get a DateTime object of the commit
    uri = URI.parse(API_ROOT + "/repos/#{organization}/#{application}/git/commits/#{git_revision}")
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    http.verify_mode = OpenSSL::SSL::VERIFY_NONE
    commit_info = JSON.parse(http.get(uri.path, headers).body)
    commit_sha_dt = DateTime.parse(commit_info["committer"]["date"])
    return commit_sha_dt
  end

  def abort_if_older_revision_unless_override(revision, organization='MasterRepos')
    revision_dt = get_datetime_for_revision(revision, organization)
    revs = get_revisions
    older = []
    revs.each_key do |h|
      deployed_dt = get_datetime_for_revision(revs[h], organization)
      older << h if (deployed_dt > revision_dt)
    end
    if (older.size > 0)
      puts "The following servers have newer revisions than currently being deployed: "
      older.each do |host|
        puts "#{host}: #{revs[host]}"
      end
      abort "ERROR: Cannot deploy older revision without specifying override=1 at end of command" unless ENV['override'] == "1"
    end
  end

  def print_revisions
    revs = get_revisions
    revs.keys.sort.each do |k|
      puts k + ": #{revs[k]}"
    end
  end

end

Capistrano.plugin :check_version, CheckVersion
