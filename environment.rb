# This file should set the project root, load the environment variables,
# and define the namespace. It *should not* do anything else. It should be
# essentially "free" to load the environment so that anything that needs
# to read ENV or know the root or use the module etc. can do it without
# worrying about "booting" the app if it didn't need to.

Thread.abort_on_exception = true

require 'dotenv'
Dotenv.load
