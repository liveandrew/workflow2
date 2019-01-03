# -*- encoding: utf-8 -*-
# stub: db_support 1.0.734 ruby lib

Gem::Specification.new do |s|
  s.name = "db_support"
  s.version = "1.0.734"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.metadata = { "allowed_push_host" => "http://gemserver.liveramp.net/" } if s.respond_to? :metadata=
  s.require_paths = ["lib"]
  s.authors = ["Ben Podgursky"]
  s.date = "2018-10-30"
  s.description = "Support for shared DB tasks"
  s.email = "dev-tools@liveramp.com"
  s.homepage = "https://git.liveramp.net/RailsRepos/db_support"
  s.rubygems_version = "2.5.1"
  s.summary = "Support for shared DB tasks"

  s.installed_by_version = "2.5.1" if s.respond_to? :installed_by_version

  if s.respond_to? :specification_version then
    s.specification_version = 4

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_development_dependency(%q<combustion>, ["~> 0.5.3"])
    else
      s.add_dependency(%q<combustion>, ["~> 0.5.3"])
    end
  else
    s.add_dependency(%q<combustion>, ["~> 0.5.3"])
  end
end
