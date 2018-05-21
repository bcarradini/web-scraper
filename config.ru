# This is basically a stub. We don't need any process to actually run for this app,
# for this project. We just need to be able to deploy the code to Heroku and then 
# invoke one-off rake tasks. In order to do that, there needs to be at least one
# process defined in the Procile. And this is that process's config file.
use Rack::Reloader, 0
use Rack::ContentLength

app = proc do |env|
  [ 200, {'Content-Type' => 'text/plain'}, ["a"] ]
end

run app
