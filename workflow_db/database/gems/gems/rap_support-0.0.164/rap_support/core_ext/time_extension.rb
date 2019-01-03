class Time

  def between(t1, t2)
    (self >= t1) && (self <= t2)
  end
end