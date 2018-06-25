require 'json'
require 'csv'
require 'httparty'
require 'timeout'
require 'fileutils'

namespace :scrape do

  def timestamp
    Time.now.strftime("%Y-%m-%d-%H:%M-UTC")
  end

  def datestamp
    Time.now.strftime("%Y-%m-%d-UTC")
  end



  #############
  # Kickstarter
  #############


  def kickstarter_base_url(region, category, sort)
    "https://www.kickstarter.com/discover/advanced?category_id=#{category}&woe_id=#{region}&sort=#{sort}"
  end


  desc 'Kickstarter Discovery'
  task :ks_discovery, [:region, :category] do |task, args|
    # Disable command
    abort("Script executed on May 29th. Don't re-run! Don't destroy the data!")

    start_time = Time.now

    DISCOVERY_SCRAPER = File.absolute_path('scrapers/ks_discovery.json')
    DISCOVERY_DIR = 'ks/discovery'
    DISCOVERY_LOGS_DIR = "#{DISCOVERY_DIR}/logs"
    DISCOVERY_OUTPUT_DIR = "#{DISCOVERY_DIR}/output"

    # Cleanup old directories and (re-)create
    # FileUtils.rm_rf DISCOVERY_DIR # Disable command
    FileUtils.mkdir_p DISCOVERY_LOGS_DIR
    FileUtils.mkdir_p DISCOVERY_OUTPUT_DIR

    # Kickstarter regions:
    #
    # 50 US States + District of Columbia:
    #   Alabama = 2347559
    #   Alaska = 2347560
    #   Arizona = 2347561
    #   Arkansas = 2347562
    #   California = 2347563
    #   Colorado = 2347564
    #   Connecticut = 2347565
    #   Delaware = 2347566
    #   District of Columbia = 2347567
    #   Florida = 2347568
    #   Georgia = 2347569
    #   Hawaii = 2347570
    #   Idaho = 2347571
    #   Illinois = 2347572
    #   Indiana = 2347573
    #   Iowa = 2347574
    #   Kansas = 2347575
    #   Kentucky = 2347576
    #   Louisiana = 2347577
    #   Maine = 2347578
    #   Maryland = 2347579
    #   Massachusetts = 2347580
    #   Michigan = 2347581
    #   Minnesota = 2347582
    #   Mississippi = 2347583
    #   Missouri = 2347584
    #   Montana = 2347585
    #   Nebraska = 2347586
    #   Nevada = 2347587
    #   New Hampshire = 2347588
    #   New Jersey = 2347589
    #   New Mexico = 2347590
    #   New York = 2347591
    #   North Carolina = 2347592
    #   North Dakota = 2347593
    #   Ohio = 2347594
    #   Oklahoma = 2347595
    #   Oregon = 2347596
    #   Pennsylvania = 2347597
    #   Rhode Island = 2347598
    #   South Carolina = 2347599
    #   South Dakota = 2347600
    #   Tennessee = 2347601
    #   Texas = 2347602
    #   Utah = 2347603
    #   Vermont = 2347604
    #   Virginia = 2347605
    #   Washington = 2347606
    #   West Virginia = 2347607
    #   Wisconsin = 2347608
    #   Wyoming = 2347609
    #
    # Non-US, Western Hemisphere:
    #   Canada = 23424775
    #   Central America = 24865707
    #   Mexico = 23424900
    #   South America = 24865673
    #
    # Eastern Hemisphere:
    #   Africa = 24865670
    #   Asia = 24865671
    #   Europe = 24865675
    #   Australasia = 55949069
    regions = [# 50 US States + District of Columbia:
               2347559, 2347560, 2347561, 2347562, 2347563, 2347564, 2347565, 2347566, 2347567, 2347568, 
               2347569, 2347570, 2347571, 2347572, 2347573, 2347574, 2347575, 2347576, 2347577, 2347578, 
               2347579, 2347580, 2347581, 2347582, 2347583, 2347584, 2347585, 2347586, 2347587, 2347588, 
               2347589, 2347590, 2347591, 2347592, 2347593, 2347594, 2347595, 2347596, 2347597, 2347598, 
               2347599, 2347600, 2347601, 2347602, 2347603, 2347604, 2347605, 2347606, 2347607, 2347608, 
               2347609, 
               # Non-US, Western Hemisphere:
               23424775, 24865707, 23424900, 24865673, 
               # Eastern Hemisphere:
               24865670, 24865671, 24865675, 55949069]

    # Kickstarter categories:
    #   1 = Art
    #   3 = Comics
    #   26 = Crafts
    #   6 = Dance
    #   7 = Design
    #   9 = Fashion
    #   11 = Film & Video
    #   10 = Food
    #   12 = Games
    #   13 = Journalism
    #   14 = Music
    #   15 = Photography
    #   18 = Publishing
    #   16 = Technology
    #   17 = Theater
    categories = [1, 3, 26, 6, 7, 9, 11, 10, 12, 13, 14, 15, 18, 16, 17, ]

    # Create an array to keep track of threads and include MonitorMixin so we 
    # can signal when a thread finishes and schedule a new one.
    thread_count = 24
    threads = Array.new(thread_count)
    threads.extend(MonitorMixin)

    # Add a condition on the monitored array to tell the consumer that.
    threads_available = threads.new_cond

    # Create a work queue for the producer to give work to the consumer.
    work_queue = SizedQueue.new(thread_count)

    # Add a variable to tell the consumer that we are done producing work.
    sysexit = false

    # Consumer thread: Schedule the work!
    consumer_thread = Thread.new do
      loop do
        # Stop looping when the producer is finished producing work
        break if sysexit && work_queue.length == 0
        found_index = nil

        # Lock the threads array
        threads.synchronize do
          # Wait on an available spot in the threads array. This will
          # fire every time threads_available.signal is invoked.
          threads_available.wait_while do
            threads.select {|thread| thread.nil? || thread.status == false }.length == 0
          end
          # We found an available spot; grab its index so we can use it for a new thread
          found_index = threads.rindex {|thread| thread.nil? || thread.status == false }
        end

        # Get a new unit of work from the work queue
        work = work_queue.pop
        region = work[:region]
        category = work[:category]
        url = work[:url]
        # puts "C: r #{region}: c #{category}: url #{url}"

        # Pass url to the new thread so it can use it as a parameter
        threads[found_index] = Thread.new(url) do
          # Scrape that shiz
          base = url.gsub('://','_').gsub(/[\/]/,'_')
          log = "#{DISCOVERY_LOGS_DIR}/#{region}/#{category}/#{base}_#{datestamp}"
          out_dir = "#{DISCOVERY_OUTPUT_DIR}/#{region}/#{category}"
          out_res = "#{out_dir}/#{base}/results.json"
          # Sometimes Quickscrape stalls; try up to 2 times, then move on
          for i in 1..2
            begin
              result = Timeout::timeout(60) {
                system "quickscrape "+
                          "--url '#{url}' "+
                          "--scraper #{DISCOVERY_SCRAPER} "+
                          "--output #{out_dir} "+
                          "--loglevel error > #{log}"
              }
            rescue Timeout::Error => e
              next
            end
            next unless result
          end
          # Process result
          if !result
            puts "C: FAILED: #{url}"
          end
          # Mark thread as finished and tell consumer to check the thread array
          Thread.current["finished"] = true
          threads.synchronize do
            threads_available.signal
          end
        end
      end
    end

    # Consumer thread: Queue the work!
    producer_thread = Thread.new do

      # Cycle through the Kickstarter Discovery pages, harvesting project URLs
      regions.each do |region|
        next if (args[:region] && args[:region].to_i != region)

        categories.each do |category|
          next if (args[:category] && args[:category].to_i != category)

          FileUtils.mkdir_p "#{DISCOVERY_LOGS_DIR}/#{region}/#{category}"
          FileUtils.mkdir_p "#{DISCOVERY_OUTPUT_DIR}/#{region}/#{category}"
          base_url = kickstarter_base_url(region, category, 'newest')

          # Setup base URL and cycle through up to 200 discovery pages
          for page in 1..200
            url = base_url + "&page=#{page}"
            # Occasionally verify that we haven't exhausted projects for this region/category 
            # (Kickstarter will keep serving pages with empty projects_list div).
            if (page % 10) == 0
              response = HTTParty.get(url)
              break if response.parsed_response.match /\<div.*id=\"projects_list\">[[:space:]]*<\/div>/
            end
            # Log progress
            if (page % 100) == 1
              puts "P: r #{region}: c #{category}: page #{page}"
            end
            # Queue URL for scraping, then tell consumer to check the thread array
            work_queue.push({region: region, category: category, url: url})
            threads.synchronize do
              threads_available.signal
            end
          end
        end
      end

      # Tell the consumer that we are finished downloading currencies
      sysexit = true
    end

    # Join on the producer and consumer threads, and on the child threads,
    # to make sure all work is complete before exiting.
    producer_thread.join
    begin
      consumer_thread.join
    rescue Exception => e
      puts e
    end
    threads.each do |thread|
        thread.join unless thread.nil?
    end

    # Calculate execution time 
    execution_time = (Time.now - start_time).round(0)
    puts "Execution Time: #{Time.at(execution_time).utc.strftime("%H:%M:%S")}"
  end


  desc 'Kickstarter Projects'
  task :ks_projects, [:region, :category] do |task, args|
    start_time = Time.now

    # This script expects 'ks/logs/' and 'ks/output/' to already exist
    DISCOVERY_OUTPUT_DIR = 'ks/discovery/output'
    PROJECTS_SCRAPER = File.absolute_path('scrapers/ks_projects.json')
    PROJECTS_DIR = 'ks/projects'
    PROJECTS_LOGS_DIR = "#{PROJECTS_DIR}/logs"
    PROJECTS_OUTPUT_DIR = "#{PROJECTS_DIR}/output"

    # Cleanup old directories and (re-)create
    # FileUtils.rm_rf PROJECTS_DIR # Disable command
    FileUtils.mkdir_p PROJECTS_LOGS_DIR
    FileUtils.mkdir_p PROJECTS_OUTPUT_DIR

    # This can be adjusted to pause/restart the process. The key is the region 
    # (as a string) and the value for each key is an array of categories that
    # have already been scraped.
    already_scraped = {'2347564' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347575' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347605' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '23424900' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347595' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347581' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347572' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347600' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347569' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347574' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347566' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347584' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347577' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347603' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347609' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '23424775' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '24865670' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347562' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347608' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347591' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347561' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347565' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347598' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347590' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347570' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347593' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347597' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '24865707' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347578' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347579' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347583' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347596' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347573' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347587' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347594' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347571' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347601' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '2347580' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                       '55949069' => ['6', '17', '10', '14', '12', '13', '26', '7', '16', '9', '18', '1', '3', '15', '11'],
                      }

    # Create an array to keep track of threads and include MonitorMixin so we 
    # can signal when a thread finishes and schedule a new one.
    thread_count = 24
    threads = Array.new(thread_count)
    threads.extend(MonitorMixin)

    # Add a condition on the monitored array to tell the consumer that.
    threads_available = threads.new_cond

    # Create a work queue for the producer to give work to the consumer.
    work_queue = SizedQueue.new(thread_count)

    # Add a variable to tell the consumer that we are done producing work.
    sysexit = false

    # Consumer thread: Schedule the work!
    consumer_thread = Thread.new do
      loop do
        # Stop looping when the producer is finished producing work
        break if sysexit && work_queue.length == 0
        found_index = nil

        # Lock the threads array
        threads.synchronize do
          # Wait on an available spot in the threads array. This will
          # fire every time threads_available.signal is invoked.
          threads_available.wait_while do
            threads.select {|thread| thread.nil? || thread.status == false }.length == 0
          end
          # We found an available spot; grab its index so we can use it for a new thread
          found_index = threads.rindex {|thread| thread.nil? || thread.status == false }
        end

        # Get a new unit of work from the work queue
        work = work_queue.pop
        region = work[:region]
        category = work[:category]
        url = work[:url]
        # puts "C: r #{region}: c #{category}: url #{url}"

        # Pass url to the new thread so it can use it as a parameter
        threads[found_index] = Thread.new(url) do
          # Scrape that shiz
          base = url.gsub('://','_').gsub(/[\/]/,'_')
          log = "#{PROJECTS_LOGS_DIR}/#{region}/#{category}/#{base}_#{datestamp}"
          out_dir = "#{PROJECTS_OUTPUT_DIR}/#{region}/#{category}"
          out_res = "#{out_dir}/#{base}/results.json"
          # Sometimes Quickscrape stalls; try up to 2 times, then move on
          for i in 1..2
            begin
              result = Timeout::timeout(300) {
                system "quickscrape "+
                          "--url '#{url}' "+
                          "--scraper #{PROJECTS_SCRAPER} "+
                          "--output #{out_dir} "+
                          "--loglevel error > #{log}"
              }
            rescue Timeout::Error => e
              next
            end
            next unless result
          end
          # Process result
          if !result
            puts "C: FAILED: #{url}"
          end
          # Mark thread as finished and tell consumer to check the thread array
          Thread.current["finished"] = true
          threads.synchronize do
            threads_available.signal
          end
        end
      end
    end

    # Consumer thread: Queue the work!
    producer_thread = Thread.new do

      # Cycle over project URLs scraped from the Kickstarter Discovery pages
      Dir.glob("#{DISCOVERY_OUTPUT_DIR}/*").each do |discovery_region_dir| 
        region = discovery_region_dir.gsub(/#{DISCOVERY_OUTPUT_DIR}\//,'')
        next if (args[:region] && args[:region] != region)

        puts "P: r #{region}: begin region"

        Dir.glob("#{discovery_region_dir}/*").each do |discovery_category_dir|
          category = discovery_category_dir.gsub(/#{DISCOVERY_OUTPUT_DIR}\/.*\//,'')
          next if (args[:category] && args[:category] != category)
          next if already_scraped[region] && already_scraped[region].include?(category)

          FileUtils.mkdir_p "#{PROJECTS_LOGS_DIR}/#{region}/#{category}"
          FileUtils.mkdir_p "#{PROJECTS_OUTPUT_DIR}/#{region}/#{category}"

          Dir.glob("#{discovery_category_dir}/*").each_with_index do |discovery_page_dir, idx|
            page = discovery_page_dir.match(/page=.*/).to_s.gsub(/page=/,'').to_i
            urls = File.read(discovery_page_dir+'/results.json')
            urls = JSON.parse(urls)['project_url']['value']

            # Log progress
            if (idx % 10) == 1
              puts "P: r #{region}: c #{category}: idx #{idx}"
            end

            # Cycle over individual URLs and queue them up
            urls.each do |url|
              # Queue URL for scraping, then tell consumer to check the thread array
              work_queue.push({region: region, category: category, url: url})
              threads.synchronize do
                threads_available.signal
              end
            end
          end
        end
      end

      # Tell the consumer that we are finished downloading currencies
      sysexit = true
    end

    # Join on the producer and consumer threads
    producer_thread.join
    begin
      consumer_thread.join
    rescue Exception => e
      puts e
    end

    # Calculate prelim execution time 
    execution_time = (Time.now - start_time).round(0)
    puts "\nExecution Time (prelim): #{Time.at(execution_time).utc.strftime("%H:%M:%S")}"

    # Join on the the child threads, to make sure all work is complete before exiting.
    threads.each do |thread|
        thread.join unless thread.nil?
    end

    # Calculate final execution time 
    execution_time = (Time.now - start_time).round(0)
    puts "Execution Time (final): #{Time.at(execution_time).utc.strftime("%H:%M:%S")}"
  end


  desc 'Kickstarter Process'
  task :ks_process do
    start_time = Time.now

    PROJECTS_LOGS_DIR = 'ks/projects/logs'
    PROJECTS_OUTPUT_DIR = 'ks/projects/output'
    KS_DIR = 'ks/'

    # Prepare CSV file for final output 
    final_csv = File.join(OUTPUT_DIR, "final_output_#{timestamp}.csv")
    csv = CSV.open(final_csv, "wb")
    csv << ['URL', 'scraped', 'state', 'title', 'sub_title', 'creator', 'first_time_creator', 'project_image', 'project_video',
            'content_images', 'content_videos', 'content_links', 'content_headers', 'content_full_description',
            'content_risks', 'pledges_amounts', 'pledge_titles', 'pledge_descriptions', 'pledge_extras', 'goal_amount',
            'pledged_amount', 'backers', 'end_date']

    projects = Dir.glob("#{PROJECTS_OUTPUT_DIR}/https_www.kickstarter.*")

    # TODO: In post-processing, distinguish "/projects/1469044562/septaer/showcase" links from 
    #   other content links. These "showcase" links are for prototype galleries
    # 

    # TODO: In post-processing, weed out standard content_links:
    #   "/help/faq/kickstarter%20basics#Acco",
    #   "/projects/1469044562/septaer/faqs",
    #   "/login?then=%2Fprojects%2F1469044562%2Fseptaer"

    # Cycle over abstracts scraped from the abstract links
    Dir.glob("#{PROJECTS_OUTPUT_DIR}/https_www.kickstarter.*") do |project|
      puts project
      file = File.read project+'/results.json'
      json = JSON.parse(file)

      # Derive URL from project directory
      url = project[/http.*/].gsub(/http_/,'http://').gsub(/_/, '/')

      # Derive scraped datestamp from associated logfile (YYYY-MM-DD)
      log = Dir.glob("#{project.gsub('output','logs')}*")[0]
      scraped = log.match(/\d\d\d\d-\d\d-\d\d-UTC/).to_s[0..9]

      # Retrieve basic project data from results file
      if json['pledged_amount_funded']['value'].empty?
        if json['end_date']['value'].empty?
          state = 'failed'
        else
          state = 'active'
        end
      else
        state = 'succeeded'
      end

      # TODO: pledge_title won't always be available

      puts "URL: #{url}"
      puts "scraped: #{scraped}"
      puts "state: #{state}"

      csv << [url, scraped, state]
      break
    end
  end

end
