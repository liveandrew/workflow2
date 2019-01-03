# Provide a to_fon function because to_f returns zero for nils and invalid strings.
# "to_fon" may stand for to "f or nil". But then again, it may not.

class NilClass
  def to_fon
    nil
  end
end

class Float
  def to_fon
    self
  end
end

class String
  def to_fon
    Float(self) rescue nil
  end
end

