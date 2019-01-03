class Cookie_types
  def self.get_autoload_dirs
    rtrn = Array[ File.expand_path(File.dirname(__FILE__)) + "/cookie_types" ]
    return rtrn
  end

  def self.get_rake_path
    File.expand_path(File.dirname(__FILE__) + "/cookie_types")
  end
end
