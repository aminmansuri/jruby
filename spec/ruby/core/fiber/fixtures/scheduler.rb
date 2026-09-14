module FiberSpecs

  class LoggingScheduler
    attr_reader :events
    def initialize
      @events = []
    end

    def block(*args)
      @events << { event: :block, fiber: Fiber.current, args: args }
      Fiber.yield
    end

    def fiber(*args, &block)
      @events << { event: :fiber, fiber: Fiber.current, args: args }
      Fiber.new(blocking: false, &block).tap(&:resume)
    end

    def io_wait(*args)
      @events << { event: :io_wait, fiber: Fiber.current, args: args }
      Fiber.yield
    end

    def kernel_sleep(*args)
      @events << { event: :kernel_sleep, fiber: Fiber.current, args: args }
      Fiber.yield
    end

    def unblock(*args)
      @events << { event: :unblock, fiber: Fiber.current, args: args }
      Fiber.yield
    end

    def fiber_interrupt(*args)
      @events << { event: :fiber_interrupt, fiber: Fiber.current, args: args }
      Fiber.yield
    end
  end

  # The shape of CRuby's test/fiber/scheduler.rb: #fiber enters the new Fiber by transfer, every
  # blocking hook parks the calling Fiber by transferring back to the Fiber that owns the scheduler,
  # and #run transfers into each parked Fiber once its timer is due.
  class TransferringScheduler
    attr_reader :waiting

    def initialize
      @owner = Fiber.current
      @waiting = {}
      @blocking = {}
      @ready = []
      @lock = Mutex.new
    end

    def fiber(&block)
      fiber = Fiber.new(blocking: false, &block)
      fiber.transfer
      fiber
    end

    def kernel_sleep(duration = nil)
      block(:sleep, duration)
    end

    def io_wait(io, events, timeout)
      block(io, timeout)
    end

    def block(blocker, timeout = nil)
      fiber = Fiber.current
      if timeout
        @waiting[fiber] = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
      else
        @blocking[fiber] = true
      end
      @owner.transfer
    ensure
      @waiting.delete(fiber)
      @blocking.delete(fiber)
    end

    def unblock(blocker, fiber)
      @lock.synchronize { @ready << fiber }
    end

    def run
      while @waiting.any? || @blocking.any? || @ready.any?
        now = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        due = @waiting.select { |_fiber, time| time <= now }.keys
        ready = @lock.synchronize { r, @ready = @ready, []; r }

        if due.empty? && ready.empty?
          soonest = @waiting.values.min
          sleep(soonest - now) if soonest && soonest > now
          next
        end

        (due + ready).each { |fiber| fiber.transfer if fiber.alive? }
      end
    end

    def close
      run
    end
  end

end
