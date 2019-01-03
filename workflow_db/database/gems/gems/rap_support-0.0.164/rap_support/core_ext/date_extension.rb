require 'date'

# Use american_date so that Date.parse can continue parse in 'MM/DD/YYYY'
# format, similar to Ruby 1.8.7.
require 'american_date'

# This extension is meant to speed up the original Date._parse method.
# We've seen ~ 10x improvement in parse times with simple testing

class Date
  class << self
    alias :_slow_parse :_parse

    def _parse(string, comp=false)
      if string =~ /^(\d+)-(\d+)-(\d+)$/
        return {:year=>$1.to_i, :mon=>$2.to_i, :mday=>$3.to_i }
      elsif string =~ /^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$/
        return {:year=>$1.to_i, :mon=>$2.to_i,
                :mday=>$3.to_i, :hour=>$4.to_i,
                :min=>$5.to_i, :sec=>$6.to_i }
      else
        a = _slow_parse(string, comp)
        return a
      end
    end
  end
end
