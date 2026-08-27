exclude :test_autoload, "autoload holds a Java monitor across Fiber switches, so a sibling fiber hangs instead of deferring to the scheduler"
exclude :test_condition_variable, "hangs on macos M1"
exclude :test_current_scheduler, "fails intermittently on Linux CI on GHA"
