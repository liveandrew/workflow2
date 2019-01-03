# Provide a to_ion function because to_i returns zero for nils and invalid strings.
# "to_ion" may stand for to "i or nil". But then again, it may not.

class NilClass
  def to_ion
    nil
  end
end

class Integer
  def to_ion
    self
  end
end

class String
  def to_ion
    Integer(self) rescue nil
  end
end

class DateTime
  def to_ion
    Integer(self) rescue nil
  end
end

class Time
  def to_ion
    Integer(self) rescue nil
  end
end
