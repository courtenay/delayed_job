$:.unshift(File.dirname(__FILE__) + '/../lib')

require 'rubygems'
require 'spec'
require 'logger'

gem 'rails', '~>2.3.5'

require 'delayed_job'
require 'sample_jobs'

Delayed::Worker.logger = Logger.new('/tmp/dj.log')
RAILS_ENV = 'test'

module Rails
  class Cache < Hash
    def read(key)
      self[key]
    end
  end
  def self.cache; @cache ||= Cache.new; end
end

# determine the available backends
BACKENDS = []
Dir.glob("#{File.dirname(__FILE__)}/setup/*.rb") do |backend|
  begin
    backend = File.basename(backend, '.rb')
    require "setup/#{backend}"
    require "backend/#{backend}_job_spec"
    BACKENDS << backend.to_sym
  rescue LoadError, Exception
    puts "Unable to load #{backend} backend! #{$!}"
  end
end

Delayed::Worker.backend = BACKENDS.first
