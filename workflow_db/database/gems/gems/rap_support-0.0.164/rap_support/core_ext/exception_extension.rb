class Exception

  def pretty
    "#{Time.now.strftime('%Y-%m-%d %H:%M:%S')} - #{message}\n#{backtrace.join("\n")}"
  end

end
