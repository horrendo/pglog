require "spec"
require "../src/pglog"

module Pglog::Spec
  extend self

  def to_time(ts : String) : Time
    Time.parse(ts, "%F %T.%L", Time::Location::UTC)
  end
end
