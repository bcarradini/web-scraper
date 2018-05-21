require 'json'
require 'csv'
require 'httparty'
require 'timeout'

namespace :scrape do
  SAGE_URL = 'http://journals.sagepub.com'

  def timestamp
    Time.now.strftime("%Y-%m-%d-%H:%M-UTC")
  end

  def datestamp
    Time.now.strftime("%Y-%m-%d-UTC")
  end



  #############
  # Misc
  #############

  desc 'Check environment'
  task :env do
    puts ENV['APP_ENV']
  end


  #############
  # BPCQ
  #############

  desc 'bpcq'
  task :bpcq do
    LOGS_DIR = 'logs/bpcq'
    OUTPUT_DIR = 'output/bpcq'
    LINKS_SCRAPER = File.absolute_path('scrapers/bpcq_contents.json')
    LINKS_LOGS_DIR = 'logs/bpcq/links'
    LINKS_OUTPUT_DIR = 'output/bpcq/links'
    ABSTRACTS_SCRAPER = File.absolute_path('scrapers/bpcq_abstracts.json')
    ABSTRACTS_LOGS_DIR = 'logs/bpcq/abstracts'
    ABSTRACTS_OUTPUT_DIR = 'output/bpcq/abstracts'

    # Cleanup old directories and (re-)create
    system "rm -rf #{LINKS_OUTPUT_DIR}/"
    system "rm -rf #{ABSTRACTS_OUTPUT_DIR}/"
    system "rm #{LINKS_LOGS_DIR}/*"
    system "rm #{ABSTRACTS_LOGS_DIR}/*"
    system "mkdir #{OUTPUT_DIR}/"
    system "mkdir #{LINKS_OUTPUT_DIR}/"
    system "mkdir #{ABSTRACTS_OUTPUT_DIR}/"
    system "mkdir #{LOGS_DIR}/"
    system "mkdir #{LINKS_LOGS_DIR}/"
    system "mkdir #{ABSTRACTS_LOGS_DIR}/"

    # Start at Volume 57, Issue 3 
    first_volume, first_issue = 57, 3
    # End at Volume 80, Issue 3 
    last_volume, last_issue = 80, 3

    issue = first_issue

    # Cycle over volumes and issues, scraping abstract links from the TOCs
    for volume in first_volume..last_volume
      while (((volume <  last_volume) && (issue <= 4)) ||
             ((volume == last_volume) && (issue <= last_issue)))
        puts "\nScraping volume #{volume}, issue #{issue}"
        result = system "quickscrape --url #{SAGE_URL}/toc/bcqe/#{volume}/#{issue} "+
                                    "--scraper #{LINKS_SCRAPER} "+
                                    "--output #{LINKS_OUTPUT_DIR} > #{LINKS_LOGS_DIR}/#{volume}_#{issue}"
        puts "ERROR: Failed to scrape volume #{volume}, issue #{issue}..." unless result
        issue += 1
      end
      volume += 1
      issue = 1
    end

    # Cycle over abstract links scraped from the TOCs
    Dir.glob("#{LINKS_OUTPUT_DIR}/http_journals.*") do |links|
      puts "\nProcessing scraped data at #{links}"
      file = File.read links+'/results.json'
      json = JSON.parse(file)
      log_cnt = 0

      # Cycle over individual links and scrape each abstract
      json['abstract_link']['value'].each do |link|
        puts "  Scraping #{link}"
        result = system "quickscrape --url #{SAGE_URL}#{link} "+
                                    "--scraper #{ABSTRACTS_SCRAPER} "+
                                    "--output #{ABSTRACTS_OUTPUT_DIR} > #{ABSTRACTS_LOGS_DIR}/#{log_cnt}"
        log_cnt += 1
      end
    end

    # Prepare CSV file for final output 
    final_csv = File.join(OUTPUT_DIR, "final_output_#{timestamp}.csv")
    csv = CSV.open(final_csv, "wb")
    csv << ['URL', 'Title', 'Author', 'Abstract']

    # Cycle over abstracts scraped from the abstract links
    Dir.glob("#{ABSTRACTS_OUTPUT_DIR}/http_journals.*") do |abstract|
      puts "\nProcessing scraped data at #{abstract}"
      file = File.read abstract+'/results.json'
      json = JSON.parse(file)

      # Retrieve and process abstract data
      url = abstract[/http.*/].gsub(/http_/,'http://').gsub(/_/, '/')
      title = json['title']['value'][0]
      authors = json['author']['value'].join(', ')
      text = json['abstract']['value'][0]

      if text && !authors.empty?
        puts "  Abstract available"
        csv << [url, title, authors, text]
      elsif authors.empty?
        puts "  No author(s) available"
      else
        puts "  No abstract available"
      end
    end

    puts "\nFinal output available at #{final_csv}"

  end



  #############
  # Kickstarter
  #############


  def kickstarter_base_url(region, category, sort)
    "https://www.kickstarter.com/discover/advanced?category_id=#{category}&woe_id=#{region}&sort=#{sort}"
  end


  desc 'Kickstarter Discovery'
  task :ks_discovery do
    start_time = Time.now

    # This script expects 'logs/ks/' and 'output/ks/' to already exist
    DISCOVERY_SCRAPER = File.absolute_path('scrapers/ks_discovery.json')
    DISCOVERY_LOGS_DIR = 'logs/ks/discovery'
    DISCOVERY_OUTPUT_DIR = 'output/ks/discovery'

    # Cleanup old directories and (re-)create
    system "rm -rf #{DISCOVERY_OUTPUT_DIR}/"
    system "rm -rf #{DISCOVERY_LOGS_DIR}"
    system "mkdir #{DISCOVERY_OUTPUT_DIR}/"
    system "mkdir #{DISCOVERY_LOGS_DIR}/"

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
    # regions = [# 50 US States + District of Columbia:
    #            2347559, 2347560, 2347561, 2347562, 2347563, 2347564, 2347565, 2347566, 2347567, 2347568, 
    #            2347569, 2347570, 2347571, 2347572, 2347573, 2347574, 2347575, 2347576, 2347577, 2347578, 
    #            2347579, 2347580, 2347581, 2347582, 2347583, 2347584, 2347585, 2347586, 2347587, 2347588, 
    #            2347589, 2347590, 2347591, 2347592, 2347593, 2347594, 2347595, 2347596, 2347597, 2347598, 
    #            2347599, 2347600, 2347601, 2347602, 2347603, 2347604, 2347605, 2347606, 2347607, 2347608, 
    #            2347609, 
    #            # Non-US, Western Hemisphere:
    #            23424775, 24865707, 23424900, 24865673, 
    #            # Eastern Hemisphere:
    #            24865670, 24865671, 24865675, 55949069]
    regions = [2347559] # TODO: Remove after testing

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
    # categories = [1, 3, 26, 6, 7, 9, 11, 10, 12, 13, 14, 15, 18, 16, 17, ]
    categories = [1, ]

    # Create an array to keep track of threads and include MonitorMixin so we 
    # can signal when a thread finishes and schedule a new one
    thread_count = 10
    threads = Array.new(thread_count)
    threads.extend(MonitorMixin)

    # Add a condition on the monitored array to tell the consumer that
    threads_available = threads.new_cond

    # Create a work queue for the producer to give work to the consumer
    work_queue = SizedQueue.new(thread_count)

    # Add a variable to tell the consumer that we are done producing work
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
        url = work_queue.pop
        puts "C: URL: #{url}"

        # Pass url to the new thread so it can use it as a parameter
        threads[found_index] = Thread.new(url) do
          # Scrape that shiz
          log = url.gsub('://','_').gsub(/[\/&]/,'_')+"_"+datestamp
          # Sometimes Quickscrape stalls; try up to 2 times, then move on
          for i in 1..2
            begin
              result = Timeout::timeout(60) {
                system "quickscrape --url '#{url}' "+
                                   "--scraper #{DISCOVERY_SCRAPER} "+
                                   "--output #{DISCOVERY_OUTPUT_DIR} "+
                                   "--loglevel error > #{DISCOVERY_LOGS_DIR}/#{log}"
              }
            rescue Timeout::Error => e
              next
            end
            next unless result
          end
          puts "C: FAILED: #{url}" unless result
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
        categories.each do |category|
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
            # Queue URL for scraping, then tell consumer to check the thread array
            work_queue.push(url)
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
    puts "\nExecution Time: #{Time.at(execution_time).utc.strftime("%H:%M:%S")}\n"
  end


  desc 'Kickstarter Projects'
  task :ks_projects do
    start_time = Time.now

    # This script expects 'logs/ks/' and 'output/ks/' to already exist
    DISCOVERY_OUTPUT_DIR = 'output/ks/discovery'
    PROJECTS_SCRAPER = File.absolute_path('scrapers/ks_project.json')
    PROJECTS_LOGS_DIR = 'logs/ks/projects'
    PROJECTS_OUTPUT_DIR = 'output/ks/projects'

    # Cleanup old directories and (re-)create
    system "rm -rf #{PROJECTS_OUTPUT_DIR}/"
    system "rm #{PROJECTS_LOGS_DIR}/*"
    system "mkdir #{PROJECTS_OUTPUT_DIR}/"
    system "mkdir #{PROJECTS_LOGS_DIR}/"

    # Create an array to keep track of threads and include MonitorMixin so we 
    # can signal when a thread finishes and schedule a new one
    thread_count = 10
    threads = Array.new(thread_count)
    threads.extend(MonitorMixin)

    # Add a condition on the monitored array to tell the consumer that
    threads_available = threads.new_cond

    # Create a work queue for the producer to give work to the consumer
    work_queue = SizedQueue.new(thread_count)

    # Add a variable to tell the consumer that we are done producing work
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
        url = work_queue.pop
        puts "C: URL: #{url}"

        # Pass url to the new thread so it can use it as a parameter
        threads[found_index] = Thread.new(url) do
          # Scrape that shiz
          log = url.gsub('://','_').gsub(/[\/&]/,'_')+"_"+datestamp
          # Sometimes Quickscrape stalls; try up to 2 times, then move on
          for i in 1..2
            begin
              result = Timeout::timeout(60) {
                system "quickscrape --url '#{url}' "+
                                   "--scraper #{PROJECTS_SCRAPER} "+
                                   "--output #{PROJECTS_OUTPUT_DIR} "+
                                   "--loglevel error > #{PROJECTS_LOGS_DIR}/#{log}"
              }
            rescue Timeout::Error => e
              next
            end
            next unless result
          end
          puts "C: FAILED: #{url}" unless result
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
      Dir.glob("#{DISCOVERY_OUTPUT_DIR}/https_www.kickstarter.*") do |discovery|
        urls = JSON.parse(File.read(discovery+'/results.json'))

        # Cycle over individual URLs and queue them up
        urls['project_url']['value'].each do |url|
          # Queue URL for scraping
          work_queue.push(url)
          # Tell consumer to check the thread array
          threads.synchronize do
            threads_available.signal
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
    puts "\nExecution Time: #{Time.at(execution_time).utc.strftime("%H:%M:%S")}\n"

    # TODO: In post-processing, distinguish "/projects/1469044562/septaer/showcase" links from 
    #   other content links. These "showcase" links are for prototype galleries
    # 

    # TODO: Not getting pledge_amount for pledge_title for Pebble 

    # TODO: In post-processing, weed out standard content_links:
    #   "/help/faq/kickstarter%20basics#Acco",
    #   "/projects/1469044562/septaer/faqs",
    #   "/login?then=%2Fprojects%2F1469044562%2Fseptaer"
  end


  desc 'Kickstarter Process'
  task :ks_process do

    # This script expects 'logs/ks/' and 'output/ks/' to already exist
    PROJECTS_LOGS_DIR = 'logs/ks/projects'
    PROJECTS_OUTPUT_DIR = 'output/ks/projects'
    OUTPUT_DIR = 'output/ks'

    # Prepare CSV file for final output 
    final_csv = File.join(OUTPUT_DIR, "final_output_#{timestamp}.csv")
    csv = CSV.open(final_csv, "wb")
    csv << ['URL', 'scraped', 'state', 'title', 'sub_title', 'creator', 'first_time_creator', 'project_image', 'project_video',
            'content_images', 'content_videos', 'content_links', 'content_headers', 'content_full_description',
            'content_risks', 'pledges_amounts', 'pledge_titles', 'pledge_descriptions', 'pledge_extras', 'goal_amount',
            'pledged_amount', 'backers', 'end_date']

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
