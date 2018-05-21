# require 'rake/testtask'
require_relative 'environment'

Dir['./tasks/**/*.rake'].each do |file|
  load file
end

task :default => [:test]
