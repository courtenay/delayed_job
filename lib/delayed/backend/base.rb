module Delayed
  module Backend
    module Base
      def self.included(base)
        base.extend ClassMethods
      end
      
      module ClassMethods
        # Add a job to the queue
        def enqueue(*args)
          object = args.shift
          unless object.respond_to?(:perform)
            raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
          end
    
          priority = args.first || Delayed::Worker.default_priority
          run_at   = args[1]
          
          # Some jobs must be run on a specific host
          server   = args[2]
          
          # sometimes we create many jobs that do the same thing
          # but may have slightly different signatures. So we override this
          # in those classes to prevent creating dupes.
          unique_key = object.unique_key
          
          # sometimes we try to serialize something weird
          # in instance variables; clear these out too
          object.clear_instance_vars! if object.respond_to?(:clear_instance_vars!)
          
          unless !unique_key.blank? && Job.exists?(:unique_key => unique_key)
            begin
              self.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at, :server => server, :unique_key => unique_key)
            rescue ActiveRecord::StatementInvalid => e
              if e.message =~ /Mysql2::Error: Duplicate entry '.*' for key 'index_delayed_jobs_on_unique_key'/
                # Do nothing. This is very unlikely, but if a lot of stuff
                # is saved at the same time, it's possible a job has been
                # created with the same key between the time we checked and
                # the time we try to create ours.
                # We also have an index on this table that normally people don't have
                # just because of idiots who sent a million emails to us from their
                # error reporting app
              else
                raise e
              end
            end
          end
        end

        def reserve(worker, max_run_time = Worker.max_run_time)
          # We get up to 5 jobs from the db. In case we cannot get exclusive access to a job we try the next.
          # this leads to a more even distribution of jobs across the worker processes
          find_available(worker.name, 5, max_run_time).detect do |job|
            job.lock_exclusively!(max_run_time, worker.name)
          end
        end

        # Hook method that is called before a new worker is forked
        def before_fork
        end
        
        # Hook method that is called after a new worker is forked
        def after_fork
        end
        
        def work_off(num = 100)
          warn "[DEPRECATION] `Delayed::Job.work_off` is deprecated. Use `Delayed::Worker.new.work_off instead."
          Delayed::Worker.new.work_off(num)
        end
      end
      
      ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

      def failed?
        failed_at
      end
      alias_method :failed, :failed?

      def name
        @name ||= begin
          payload = payload_object
          if payload.respond_to?(:display_name)
            payload.display_name
          else
            payload.class.name
          end
        end
      end

      def payload_object=(object)
        self['handler'] = object.to_yaml
      end
      
      def payload_object
        @payload_object ||= deserialize(self['handler'])
      end

      # Moved into its own method so that new_relic can trace it.
      def invoke_job
        # Sometimes instance variables need to be reset because of serialization issues
        if payload_object.respond_to?(:clear_instance_vars!)
          payload_object.clear_instance_vars!
        end

        # set the delayed_job ID (when supported) so we can do
        # evil introspection in the payload instance
        Marginalia::Comment.set_job!(self) if defined?(Marginalia)
        payload_object.delayed_job_id = id if payload_object.respond_to? :delayed_job_id=

        payload_object.perform
      end
      
      # Unlock this job (note: not saved to DB)
      def unlock
        self.locked_at    = nil
        self.locked_by    = nil
      end
    
      def reschedule_at
        payload_object.respond_to?(:reschedule_at) ? 
          payload_object.reschedule_at(self.class.db_time_now, attempts) :
          self.class.db_time_now + (attempts ** 4) + 5
      end

      def max_attempts
        payload_object.max_attempts if payload_object.respond_to?(:max_attempts)
      end
      
    private

      def deserialize(source)
        handler = YAML.load(source) rescue nil

        unless handler.respond_to?(:perform)
          if handler.nil? && source =~ ParseObjectFromYaml
            handler_class = $1
          end
          attempt_to_load(handler_class || handler.class)
          handler = YAML.load(source)
        end

        return handler if handler.respond_to?(:perform)

        raise DeserializationError,
          'Job failed to load: Unknown handler. Try to manually require the appropriate file.'

        # Rescue and rethrow all the important stuff because we have a catchall after that.
        rescue NoMemoryError, LoadError, NameError, ArgumentError, ScriptError, SignalException, SystemExit => e
          throw e

        # We have to rescue Exception because Psych throws unknown errors that are not StandardError
        rescue TypeError, LoadError, NameError, ArgumentError, Exception => e
          raise DeserializationError,
            "Job failed to load: #{e.message}. Try to manually require the required file."
      end

      # Constantize the object so that ActiveSupport can attempt
      # its auto loading magic. Will raise LoadError if not successful.
      def attempt_to_load(klass)
         klass.constantize
      end
      
    protected

      def set_default_run_at
        self.run_at ||= self.class.db_time_now
      end    

    end
  end
end
