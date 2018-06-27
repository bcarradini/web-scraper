require 'csv'

namespace :process do

  desc 'split_abstracts'
  task :split_abstracts, [:file,:tag] do |t,args|
    abort('Must provide file') unless args[:file]

    abstracts = CSV.read("input/#{args[:file]}", encoding: 'iso-8859-1')
    cnt = 0

    abstracts.each do |row|
      File.write("output/split/abs_#{args[:tag]}_#{'%04d' % cnt}.txt", row[0])
      cnt += 1
    end
  end

end
