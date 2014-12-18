class SimpleJob
  cattr_accessor :runs; self.runs = 0
  def unique_key; "id-#{rand}"; end
  def perform; @@runs += 1; end
end

class ErrorJob
  cattr_accessor :runs; self.runs = 0
  def unique_key; "id-#{rand}"; end
  def perform; raise 'did not work'; end
end             

class LongRunningJob
  def unique_key; "id-#{rand}"; end
  def perform; sleep 250; end
end

class OnPermanentFailureJob < SimpleJob
  def on_permanent_failure
  end
  def max_attempts; 1; end
end

module M
  class ModuleJob
    def unique_key; "id-#{rand}"; end
    cattr_accessor :runs; self.runs = 0
    def perform; @@runs += 1; end    
  end
end
