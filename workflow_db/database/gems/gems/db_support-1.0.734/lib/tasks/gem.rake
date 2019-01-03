namespace :gem do
  task :update_gemfile_dot_lock do
    require 'digest'
    bundle_update_gems = "rapleaf_types rap_support"

    hash_before = Digest::MD5.hexdigest(File.read('Gemfile.lock'))

    update_output = `bundle update #{bundle_update_gems}`
    raise "update_gemfile_dot_lock failed:\n#{update_output}" unless $?.success?

    hash_after = Digest::MD5.hexdigest(File.read('Gemfile.lock'))

    if hash_before != hash_after
      `git add Gemfile.lock && git commit -m "Gemfile.lock automatically updated to latest version"`
      `git fetch origin && git rebase origin/master && git push origin master`
      `curl -sk -u "#{ENV['HUDSON_API_USER']}:#{ENV['HUDSON_API_PASS']}" #{ENV['BUILD_URL']}stop`
    end
  end
end
