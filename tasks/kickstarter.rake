require 'json'
require 'csv'
require 'httparty'
require 'timeout'
require 'fileutils'

namespace :kickstarter do

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
    # Disable command
    abort("Script executed June 2018. Don't re-run! Don't destroy the data!")

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

    PROJECTS_DIR = 'ks/projects'
    PROJECTS_LOGS_DIR = "#{PROJECTS_DIR}/logs"
    PROJECTS_OUTPUT_DIR = "#{PROJECTS_DIR}/output"

    # Prepare CSV file for final output 
    final_csv = File.join(PROJECTS_DIR, "final_output_#{datestamp}.csv")
    csv = CSV.open(final_csv, "wb")
    csv << ['URL', 'scraped', 'title', 'sub_title', 'status', 'goal_amount', 'pledged_amount', 'backers', 'end_date', 
            'creator', 'first_time_creator', 'project_image', 'project_video', 'project_galleries', 'content_images_nongif', 'content_images_gif', 
            'content_videos', 'content_links', 'content_headers', 'content_full_description', 'content_risks', 
            'pledge_amount_0', 'pledge_amount_1', 'pledge_amount_2', 'pledge_amount_3', 'pledge_amount_4', 'pledge_amount_5', 'pledge_amount_6', 'pledge_amount_7', 'pledge_amount_8', 'pledge_amount_9', 
            'pledge_amount_10', 'pledge_amount_11', 'pledge_amount_12', 'pledge_amount_13', 'pledge_amount_14', 'pledge_amount_15', 'pledge_amount_16', 'pledge_amount_17', 'pledge_amount_18', 'pledge_amount_19', 
            'pledge_amount_20', 'pledge_amount_21', 'pledge_amount_22', 'pledge_amount_23', 'pledge_amount_24', 'pledge_amount_25', 'pledge_amount_26', 'pledge_amount_27', 'pledge_amount_28', 'pledge_amount_29', 
            'pledge_amount_30', 'pledge_amount_31', 'pledge_amount_32', 'pledge_amount_33', 'pledge_amount_34', 'pledge_amount_35', 'pledge_amount_36', 'pledge_amount_37', 'pledge_amount_38', 'pledge_amount_39', 
            'pledge_amount_40', 'pledge_amount_41', 'pledge_amount_42', 'pledge_amount_43', 'pledge_amount_44', 'pledge_amount_45', 'pledge_amount_46', 'pledge_amount_47', 'pledge_amount_48', 'pledge_amount_49', 
            'pledge_title_0', 'pledge_title_1', 'pledge_title_2', 'pledge_title_3', 'pledge_title_4', 'pledge_title_5', 'pledge_title_6', 'pledge_title_7', 'pledge_title_8', 'pledge_title_9', 
            'pledge_title_10', 'pledge_title_11', 'pledge_title_12', 'pledge_title_13', 'pledge_title_14', 'pledge_title_15', 'pledge_title_16', 'pledge_title_17', 'pledge_title_18', 'pledge_title_19', 
            'pledge_title_20', 'pledge_title_21', 'pledge_title_22', 'pledge_title_23', 'pledge_title_24', 'pledge_title_25', 'pledge_title_26', 'pledge_title_27', 'pledge_title_28', 'pledge_title_29', 
            'pledge_title_30', 'pledge_title_31', 'pledge_title_32', 'pledge_title_33', 'pledge_title_34', 'pledge_title_35', 'pledge_title_36', 'pledge_title_37', 'pledge_title_38', 'pledge_title_39', 
            'pledge_title_40', 'pledge_title_41', 'pledge_title_42', 'pledge_title_43', 'pledge_title_44', 'pledge_title_45', 'pledge_title_46', 'pledge_title_47', 'pledge_title_48', 'pledge_title_49', 
            'pledge_description_0', 'pledge_description_1', 'pledge_description_2', 'pledge_description_3', 'pledge_description_4', 'pledge_description_5', 'pledge_description_6', 'pledge_description_7', 'pledge_description_8', 'pledge_description_9', 
            'pledge_description_10', 'pledge_description_11', 'pledge_description_12', 'pledge_description_13', 'pledge_description_14', 'pledge_description_15', 'pledge_description_16', 'pledge_description_17', 'pledge_description_18', 'pledge_description_19', 
            'pledge_description_20', 'pledge_description_21', 'pledge_description_22', 'pledge_description_23', 'pledge_description_24', 'pledge_description_25', 'pledge_description_26', 'pledge_description_27', 'pledge_description_28', 'pledge_description_29', 
            'pledge_description_30', 'pledge_description_31', 'pledge_description_32', 'pledge_description_33', 'pledge_description_34', 'pledge_description_35', 'pledge_description_36', 'pledge_description_37', 'pledge_description_38', 'pledge_description_39', 
            'pledge_description_40', 'pledge_description_41', 'pledge_description_42', 'pledge_description_43', 'pledge_description_44', 'pledge_description_45', 'pledge_description_46', 'pledge_description_47', 'pledge_description_48', 'pledge_description_49', 
            'pledge_extras_0', 'pledge_extras_1', 'pledge_extras_2', 'pledge_extras_3', 'pledge_extras_4', 'pledge_extras_5', 'pledge_extras_6', 'pledge_extras_7', 'pledge_extras_8', 'pledge_extras_9', 
            'pledge_extras_10', 'pledge_extras_11', 'pledge_extras_12', 'pledge_extras_13', 'pledge_extras_14', 'pledge_extras_15', 'pledge_extras_16', 'pledge_extras_17', 'pledge_extras_18', 'pledge_extras_19', 
            'pledge_extras_20', 'pledge_extras_21', 'pledge_extras_22', 'pledge_extras_23', 'pledge_extras_24', 'pledge_extras_25', 'pledge_extras_26', 'pledge_extras_27', 'pledge_extras_28', 'pledge_extras_29', 
            'pledge_extras_30', 'pledge_extras_31', 'pledge_extras_32', 'pledge_extras_33', 'pledge_extras_34', 'pledge_extras_35', 'pledge_extras_36', 'pledge_extras_37', 'pledge_extras_38', 'pledge_extras_39', 
            'pledge_extras_40', 'pledge_extras_41', 'pledge_extras_42', 'pledge_extras_43', 'pledge_extras_44', 'pledge_extras_45', 'pledge_extras_46', 'pledge_extras_47', 'pledge_extras_48', 'pledge_extras_49']

    projects = Dir.glob("#{PROJECTS_OUTPUT_DIR}/")

    # Cycle over results scraped from Kickstarter Project pages
    Dir.glob("#{PROJECTS_OUTPUT_DIR}/*").each do |region| 
      puts "Region: #{region}"

      Dir.glob("#{region}/*").each do |category|
        Dir.glob("#{category}/*").each do |project|
          file = File.read project+'/results.json'
          json = JSON.parse(file)

          # Derive URL from category directory
          url = project[/http.*/].gsub(/https_/,'https://').gsub(/_/, '/')

          # Derive scraped datestamp from associated logfile (YYYY-MM-DD)
          log = Dir.glob("#{project.gsub('output','logs')}*")[0]
          scraped = log.match(/\d\d\d\d-\d\d-\d\d-UTC/).to_s[0..9]

          # Retrieve project data from results file
          title = retrieve_and_strip(json['title'])
          sub_title = retrieve_and_strip(json['sub_title'])
          status = json['pledged_amount_funded']['value'].empty? ? (json['end_date']['value'].empty? ? 'failed' : 'active') : ('succeeded')
          goal_amount = retrieve_and_strip_dollars(json['goal_amount'])
          pledged_amount = retrieve_and_strip_dollars(json['pledged_amount_unfunded']) || retrieve_and_strip_dollars(json['pledged_amount_funded'])
          backers = retrieve_and_strip(json['backers_unfunded']) || retrieve_and_strip(json['backers_funded'])&.gsub(' backers','')
          end_date = retrieve_and_strip(json['end_date'])

          # Creator info
          creator = retrieve_and_strip(json['creator'])
          first_time_creator = json['first_time_creator']['value'].empty? ? 'no' : 'yes'

          # Project "front page"
          project_image = json['project_image']['value'].count
          project_video = json['project_video']['value'].count
          project_galleries = json['content_links']['value'].map {|l| l.match(/\A\/projects.*showcase/) ? l : nil }.compact.count

          # Content
          content_images_gif = json['content_images']['value'].map {|i| i.match('.gif') ? i : nil }.compact.count
          content_images_nongif = json['content_images']['value'].count - content_images_gif
          content_videos = json['content_videos']['value'].count
          content_links = json['content_links']['value'].map do |l|
            # Begins with "/discover", "/projects", "/login", "/help"
            (l.match(/\A\/discover/) || l.match(/\A\/projects/) || l.match(/\A\/login/) || l.match(/\A\/help/)) ? nil : l
          end.compact.count
          content_headers = json['content_headers']['value'].count
          content_full_description = retrieve_and_strip(json['content_full_description'])
          content_risks = retrieve_and_strip(json['content_risks'])&.gsub(/\A\n\nRisks and challenges\n/,'')&.gsub("\n",' ')

          # Assemble preliminary row for CSV file
          row = [url, scraped, title, sub_title, status, goal_amount, pledged_amount, backers, end_date, creator, 
                 first_time_creator, project_image, project_video, project_galleries, content_images_nongif, 
                 content_images_gif, content_videos, content_links, content_headers, content_full_description, 
                 content_risks]

          # Assemble final row by processing pledge data. Fill out all expected columns in row, even if
          # data is blank. Use "&." for string cleanup so the app won't raise an exception for nil items.
          for i in 0..49
            row.push strip_dollars(json['pledge_amount']['value'][i])
          end
          for i in 0..49
            row.push json['pledge_title']['value'][i]&.strip
          end
          for i in 0..49
            row.push json['pledge_description']['value'][i]&.gsub("\n\n\nLess\n\n",'')&.gsub("\n",' ')&.strip
          end
          for i in 0..49
            row.push json['pledge_extra_info']['value'][i]&.strip
          end
          
          # Add row to CSV file
          csv << row
        end
        # TODO: Remove after testing
        break
      end
    end
  end

  def strip_dollars(amount)
    amount&.gsub(/.*\$/,'')&.gsub(',','')&.strip
  end

  def retrieve_and_strip(item_with_value_list)
    item_with_value_list['value'].map {|v| (!v || v.strip.empty?) ? nil : v.strip }.compact[0]
  end

  def retrieve_and_strip_dollars(item_with_value_list)
    strip_dollars(retrieve_and_strip(item_with_value_list))
  end

end
