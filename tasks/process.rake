require 'csv'

namespace :process do

  desc 'bpcq'
  task :split_abstracts do
    abstracts = CSV.read('input/DataToSplitIntoSeparateFiles.csv')
    cnt = 0

    abstracts.each do |row|
      abstract = CSV.open("output/split/abs_#{'%04d' % cnt}.csv", "wb")
      abstract << row
      cnt += 1
    end
  end

end
