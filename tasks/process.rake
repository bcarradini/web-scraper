require 'csv'

namespace :process do

  desc 'split_abstracts'
  task :split_abstracts do
    abstracts = CSV.read('input/DataToSplitIntoSeparateFiles.csv')
    cnt = 0

    abstracts.each do |row|
      File.write("output/split/abs_#{'%04d' % cnt}.txt", row[0])
      cnt += 1
    end
  end

  desc 'split_abstracts'
  task :split_ieee_abstracts do
    abstracts = CSV.read('input/IEEE_Abstracts_To_Turn_Into_Files.csv')
    cnt = 0

    abstracts.each do |row|
      File.write("output/split/abs_ieee_#{'%04d' % cnt}.txt", row[0])
      cnt += 1
    end
  end

end
