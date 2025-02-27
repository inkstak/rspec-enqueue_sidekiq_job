require 'spec_helper'

RSpec.describe RSpec::EnqueueSidekiqJob do
  let(:worker) do
    Class.new do
      include ::Sidekiq::Worker
    end
  end

  let(:another_worker) do
    Class.new do
      include ::Sidekiq::Worker
    end
  end

  it 'raises ArgumentError when used in value expectation', pending: 'only fails with ArgumentError on RSpec 4' do
    expect {
      expect(worker.perform_async).to enqueue_sidekiq_job(worker)
    }.to raise_error(ArgumentError)
  end

  it 'fails when no worker class is specified' do
    expect {
      expect { worker.perform_async }.to enqueue_sidekiq_job
    }.to raise_error(ArgumentError)
  end

  it 'passes' do
    expect { worker.perform_async }
      .to enqueue_sidekiq_job(worker)
  end

  it 'fails when negated and job is enqueued' do
    expect {
      expect { worker.perform_async }.not_to enqueue_sidekiq_job(worker)
    }.to raise_error(/expected not to enqueue/)
  end

  context 'when no jobs were enqueued' do
    it 'fails' do
      expect {
        expect {} # nop
          .to enqueue_sidekiq_job(worker)
      }.to raise_error(/expected to enqueue/)
    end

    it 'passes with negation' do
      expect {} # nop
        .not_to enqueue_sidekiq_job(worker)
    end
  end

  context 'with another worker' do
    it 'fails' do
      expect {
        expect { worker.perform_async }
          .to enqueue_sidekiq_job(another_worker)
      }.to raise_error(/expected to enqueue/)
    end

    it 'passes with negation' do
      expect { worker.perform_async }
        .not_to enqueue_sidekiq_job(another_worker)
    end
  end

  it 'counts only jobs enqueued in block' do
    worker.perform_async
    expect {}.not_to enqueue_sidekiq_job(worker)
  end

  it 'counts jobs enqueued in block' do
    worker.perform_async
    expect { worker.perform_async }.to enqueue_sidekiq_job(worker)
  end

  describe 'count constraints' do
    it 'fails when too many jobs enqueued' do
      expect {
        expect {
          worker.perform_async
          worker.perform_async
        }.to enqueue_sidekiq_job(worker)
      }.to raise_error(/expected to enqueue/)
    end

    it 'fails when negated and several jobs enqueued' do
      expect {
        expect {
          worker.perform_async
          worker.perform_async
        }.not_to enqueue_sidekiq_job(worker)
      }.to raise_error(/expected not to enqueue/)
    end

    it 'passes with multiple different jobs' do
      expect {
        another_worker.perform_async
        worker.perform_async
      }
        .to enqueue_sidekiq_job(worker)
        .and enqueue_sidekiq_job(another_worker)
    end

    it 'passes when explicitly expected to be enqueued once' do
      expect {
        worker.perform_async
      }.to enqueue_sidekiq_job(worker).once
    end

    it 'passes when explicitly expected to be enqueued twice' do
      expect {
        worker.perform_async
        worker.perform_async
      }.to enqueue_sidekiq_job(worker).twice
    end

    it 'fails when expected to be enqueued twice, but enqueued once' do
      expect {
        expect {
          worker.perform_async
        }.to enqueue_sidekiq_job(worker).twice
      }.to raise_error(/expected to enqueue/)
    end

    it 'passes when expected to be enqueued twice, but enqueued more than twice' do
      expect {
        expect {
          3.times { worker.perform_async }
        }.to enqueue_sidekiq_job(worker).twice
      }.to raise_error(/expected to enqueue/)
    end

    it 'fails on attempt to use negation with explicit counts' do
      expect {
        expect {}.not_to enqueue_sidekiq_job(worker).twice
      }.to raise_error(/counts are not supported with negation/)
    end

    it 'provides `exactly` and `times`' do
      expect {
        2.times { worker.perform_async }
      }.to enqueue_sidekiq_job(worker).exactly(2).times
    end
  end

  context 'when enqueued with perform_at' do
    it 'passes' do
      future = 1.minute.from_now
      expect { worker.perform_at(future) }
        .to enqueue_sidekiq_job(worker).at(future)
    end

    it 'fails when timestamps do not match' do
      future = 1.minute.from_now
      expect {
        expect { worker.perform_at(future) }
          .to enqueue_sidekiq_job(worker).at(2.minutes.from_now)
      }.to raise_error(/expected to enqueue.+at:/m)
    end

    it 'matches timestamps with nanosecond precision' do
      100.times do
        future = 1.minute.from_now
        future = future.change(nsec: future.nsec.round(-3) + rand(999))
        expect { worker.perform_at(future) }
          .to enqueue_sidekiq_job(worker).at(future)
      end
    end

    it 'accepts composable matchers' do
      future = 1.minute.from_now
      slightly_earlier = 58.seconds.from_now
      expect { worker.perform_at(slightly_earlier) }
        .to enqueue_sidekiq_job(worker).at(a_value_within(5.seconds).of(future))
    end

    it 'fails when the job was enuqued for now' do
      expect {
        expect { worker.perform_async }
          .to enqueue_sidekiq_job(worker).at(1.minute.from_now)
      }.to raise_error(/expected to enqueue.+at:/m)
    end

    it 'fails when both in and at are specified' do
      expect {
        enqueue_sidekiq_job(worker).at(1.minute.from_now).in(1.minute)
      }.to raise_error(/both `at` and `in` is not supported/)
    end
  end

  context 'when enqueued with perform_in' do
    it 'passes' do
      interval = 1.minute
      expect { worker.perform_in(interval) }
        .to enqueue_sidekiq_job(worker).in(interval)
    end

    it 'fails when timestamps do not match' do
      interval = 1.minute
      expect {
        expect { worker.perform_in(interval) }
          .to enqueue_sidekiq_job(worker).in(2.minutes)
      }.to raise_error(/expected to enqueue.+in:/m)
    end

    it 'fails when the job was enuqued for now' do
      expect {
        expect { worker.perform_async }
          .to enqueue_sidekiq_job(worker).in(1.minute)
      }.to raise_error(/expected to enqueue.+in:/m)
    end

    it 'fails when both in and at are specified' do
      expect {
        enqueue_sidekiq_job(worker).in(1.minute).at(1.minute.from_now)
      }.to raise_error(/both `at` and `in` is not supported/)
    end
  end

  it 'matches when not specified at and scheduled for the future' do
    expect { worker.perform_in(1.day) }
      .to enqueue_sidekiq_job(worker)
    expect { worker.perform_at(1.day.from_now) }
      .to enqueue_sidekiq_job(worker)
  end

  context 'with arguments' do
    it 'passes with provided arguments' do
      expect { worker.perform_async(42, 'David') }
        .to enqueue_sidekiq_job(worker).with(42, 'David')
    end

    it 'supports provided argument matchers' do
      expect { worker.perform_async(42, 'David') }
        .to enqueue_sidekiq_job(worker).with(be > 41, a_string_including('Dav'))
    end

    it 'passes when negated and arguments do not match' do
      expect { worker.perform_async(42, 'David') }
        .not_to enqueue_sidekiq_job(worker).with(11, 'Phil')
    end

    it 'fails when arguments do not match' do
      expect {
        expect { worker.perform_async(42, 'David') }
          .to enqueue_sidekiq_job(worker).with(11, 'Phil')
      }.to raise_error(/expected to enqueue.+arguments:/m)
    end
  end

  context 'with hash arguments' do
    it 'passes with symbol keys' do
      expect { worker.perform_async(42, name: 'David') }
        .to enqueue_sidekiq_job(worker).with(42, name: 'David')
    end

    it 'passes with string keys' do
      expect { worker.perform_async(42, 'name' => 'David') }
        .to enqueue_sidekiq_job(worker).with(42, 'name' => 'David')
    end

    context 'when matcher and perform tpyes are intermixed' do
      it 'passes with symbol keys' do
        expect { worker.perform_async(42, name: 'David') }
          .to enqueue_sidekiq_job(worker).with(42, 'name' => 'David')
      end

      it 'passes with string keys' do
        expect { worker.perform_async(42, 'name' => 'David') }
          .to enqueue_sidekiq_job(worker).with(42, name: 'David')
      end
    end
  end

  context 'with block arguments' do
    it 'passes with provided arguments' do
      expect {
        worker.perform_async(42, 'David')
        "David"
      }.to enqueue_sidekiq_job(worker).with { |name| [42, name] }
    end

    it 'passes when negated and arguments do not match' do
      expect expect {
        worker.perform_async(42, 'David')
        "Phil"
      }.not_to enqueue_sidekiq_job(worker).with { |name| [42, name] }
    end

    it 'fails when arguments do not match' do
      expect {
        expect {
          worker.perform_async(42, 'David')
          "Phil"
        }.to enqueue_sidekiq_job(worker).with { |name| [42, name] }
      }.to raise_error(/expected to enqueue.+arguments:/m)
    end

    it 'rejects arguments mixed with block' do
      expect {
        expect { worker.perform_async(42, 'David') }
          .to enqueue_sidekiq_job(worker).with(42) { |name| [42, name] }
      }.to raise_error(ArgumentError, "setting arguments with block is not supported")
    end

    it 'rejects arguments returned from block if they are not an Array' do
      expect {
        expect {
          worker.perform_async(42, 'David')
          "Phil"
        }.to enqueue_sidekiq_job(worker).with { |name| name }
      }.to raise_error("`with` block is expected to return an Array")
    end
  end
end
