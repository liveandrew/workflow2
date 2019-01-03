class Rapleaf_types
  def self.get_autoload_dirs
    rtrn = Array[ File.expand_path(File.dirname(__FILE__)) + "/rapleaf_types" ]
    return rtrn
  end

  def self.get_rake_path
    File.expand_path(File.dirname(__FILE__) + "/rapleaf_types")
  end
end
