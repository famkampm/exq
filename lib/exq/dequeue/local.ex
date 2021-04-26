defmodule Exq.Dequeue.Local do
  require Logger
  @behaviour Exq.Dequeue.Behaviour

  defmodule State do
    @moduledoc false

    defstruct max: nil, current: 0
  end

  @impl true
  def init(_, options) do
    {:ok, %State{max: Keyword.fetch!(options, :concurrency)}}
  end

  @impl true
  def stop(_), do: :ok

  @impl true
  def available?(state) do
    Logger.info("dequeue.local-state.available?.current:#{state.current}")
    {:ok, state.current < state.max, state}
  end

  @impl true
  def dispatched(state) do
    Logger.info("dequeue.local-state.dispatched.current:#{state.current}")
    {:ok, %{state | current: state.current + 1}}
  end

  @impl true
  def processed(state) do
    Logger.info("dequeue.local-state.processed.current:#{state.current}")
    {:ok, %{state | current: state.current - 1}}
  end

  @impl true
  def failed(state) do
    Logger.info("dequeue.local-state.failed.current:#{state.current}")
    {:ok, %{state | current: state.current - 1}}
  end
end
