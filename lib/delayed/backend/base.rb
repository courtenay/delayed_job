module Delayed
  module Backend
    module Base
      def self.included(base)
        base.extend ClassMethods
      end

      module ClassMethods
        # Add a job to the queue
        def enqueue(*args)
          options = {
            :priority => Delayed::Worker.default_priority
          }.merge!(args.extract_options!)

          options[:payload_object] ||= args.shift

          if args.size > 0
            warn "[DEPRECATION] Passing multiple arguments to `#enqueue` is deprecated. Pass a hash with :priority and :run_at."
            options[:priority] = args.first || options[:priority]
            options[:run_at]   = args[1]
            options[:server]   = args[2]
          end

          unless options[:payload_object].respond_to?(:perform)
            raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
          end
          
          # sometimes we create many jobs that do the same thing
          # but may have slightly different signatures. So we override this
          # in those classes to prevent creating dupes.
          unique_key = options[:payload_object].unique_key if options[:payload_object].respond_to?(:unique_key)
          
          # sometimes we try to serialize something weird
          # in instance variables; clear these out too
          options[:payload_object].clear_instance_vars! if options[:payload_object].respond_to?(:clear_instance_vars!)
          
          if ! unique_key.blank? && Job.exists?(:unique_key => unique_key)
            # do not create job
            return
          end
          options[:unique_key] = unique_key
          
          if Delayed::Worker.delay_jobs
            begin
              self.new(options).tap do |job|
                job.hook(:enqueue)
                job.save
              end
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
          else
            Delayed::Job.new(:payload_object => options[:payload_object]).tap do |job|
              job.invoke_job
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

      def failed?
        failed_at
      end
      alias_method :failed, :failed?

      ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

      def name
        @name ||= payload_object.respond_to?(:display_name) ?
                    payload_object.display_name :
                    payload_object.class.name
      rescue DeserializationError
        ParseObjectFromYaml.match(handler)[1]
      end

      def payload_object=(object)
        @payload_object = object
        self.handler = object.to_yaml
      end

      def payload_object
        @payload_object ||= YAML.load(self.handler)
      rescue TypeError, LoadError, NameError, ArgumentError => e
        raise DeserializationError,
          "Job failed to load: #{e.message}. Handler: #{handler.inspect}"
      end

      def invoke_job
        hook :before

        # Sometimes instance variables need to be reset because of serialization issues
        if payload_object.respond_to?(:clear_instance_vars!)
          payload_object.clear_instance_vars!
        end

        # set the delayed_job ID (when supported) so we can do
        # evil introspection in the payload instance
        Marginalia::Comment.set_job!(self) if defined?(Marginalia)
        payload_object.delayed_job_id = id if payload_object.respond_to? :delayed_job_id=

        payload_object.perform
        hook :success
      rescue Exception => e
        hook :error, e
        raise e
      ensure
        hook :after
      end

      # Unlock this job (note: not saved to DB)
      def unlock
        self.locked_at    = nil
        self.locked_by    = nil
      end

      def hook(name, *args)
        if payload_object.respond_to?(name)
          method = payload_object.method(name)
          method.arity == 0 ? method.call : method.call(self, *args)
        end
      rescue DeserializationError
        # do nothing
      end

      def reschedule_at
        payload_object.respond_to?(:reschedule_at) ?
          payload_object.reschedule_at(self.class.db_time_now, attempts) :
          self.class.db_time_now + (attempts ** 4) + 5
      end

      def max_attempts
        payload_object.max_attempts if payload_object.respond_to?(:max_attempts)
      end

    protected

      def set_default_run_at
        self.run_at ||= self.class.db_time_now
      end
    end
  end
end
