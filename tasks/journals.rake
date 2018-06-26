require 'json'
require 'csv'
require 'httparty'
require 'timeout'
require 'fileutils'

namespace :journals do
  SAGE_URL = 'http://journals.sagepub.com'

  def timestamp
    Time.now.strftime("%Y-%m-%d-%H:%M-UTC")
  end

  def datestamp
    Time.now.strftime("%Y-%m-%d-UTC")
  end



  #############
  # BPCQ
  #############

  desc 'bpcq'
  task :bpcq do
    LOGS_DIR = 'logs/bpcq'
    OUTPUT_DIR = 'output/bpcq'
    LINKS_SCRAPER = File.absolute_path('scrapers/bpcq_links.json')
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
        result = system "quickscrape "+
                          "--url #{SAGE_URL}/toc/bcqe/#{volume}/#{issue} "+
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
        result = system "quickscrape "+
                          "--url #{SAGE_URL}#{link} "+
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
  # TCQ
  #############

  desc 'TCQ'
  task :tcq do
    start_time = Time.now

    MS_URL = 'https://academic.microsoft.com/'
    TCQ_1996_2008_URL= MS_URL+'#/search?iq=@technical%20communication%20quarterly@&filters=Y%3E%3D1996%2CY%3C%3D2008'
    LOGS_DIR = 'logs/tcq'
    OUTPUT_DIR = 'output/tcq'
    LINKS_SCRAPER = File.absolute_path('scrapers/tcq_links.json')
    LINKS_LOGS_DIR = 'logs/tcq/links'
    LINKS_OUTPUT_DIR = 'output/tcq/links'
    ABSTRACTS_SCRAPER = File.absolute_path('scrapers/tcq_abstracts.json')
    ABSTRACTS_LOGS_DIR = 'logs/tcq/abstracts'
    ABSTRACTS_OUTPUT_DIR = 'output/tcq/abstracts'

    # Cleanup old directories and (re-)create
    FileUtils.mkdir_p LINKS_LOGS_DIR
    FileUtils.mkdir_p LINKS_OUTPUT_DIR
    FileUtils.mkdir_p ABSTRACTS_LOGS_DIR
    FileUtils.mkdir_p ABSTRACTS_OUTPUT_DIR

    # Start at offset 0 and work up to offset 336
    offset = 0
    last_offset = 336

    # Cycle through pages of results by offset (8 results per page)
    while offset <= last_offset
      puts "\nScraping offset #{offset}"
      keep_trying = true
      while keep_trying
        # Retrieve result
        result = system "quickscrape "+
                            "--url '#{TCQ_1996_2008_URL}&from=#{offset}&sort=2' "+
                            "--scraper #{LINKS_SCRAPER} "+
                            "--output #{LINKS_OUTPUT_DIR} > #{LINKS_LOGS_DIR}/#{offset}"
        # Check that result is valid (sometimes url fails to load properly)
        file = File.read "#{LINKS_OUTPUT_DIR}/#{TCQ_1996_2008_URL.gsub('://','/').gsub('/','_')}&from=#{offset}&sort=2/results.json"
        json = JSON.parse(file)
        if json['abstract_link']['value'][0] # If we captured at least one link, don't keep trying
          keep_trying = false
        else
          puts "    try again..."
        end
      end
      # Progress to next offset (8 results per page)
      offset += 8
    end

    # Cycle over abstract links scraped from the TOCs
    Dir.glob("#{LINKS_OUTPUT_DIR}/https_academic.*") do |links|
      puts "\nProcessing scraped data at #{links}"
      file = File.read links+'/results.json'
      json = JSON.parse(file)

      # Cycle over individual links and scrape each abstract
      json['abstract_link']['value'].each do |link|
        puts "  Scraping #{link}"
        keep_trying = true
        while keep_trying
          # Retrieve result
          result = system "quickscrape "+
                            "--url '#{MS_URL}#{link}' "+
                            "--scraper #{ABSTRACTS_SCRAPER} "+
                            "--output #{ABSTRACTS_OUTPUT_DIR} > #{ABSTRACTS_LOGS_DIR}/#{link.gsub('#/detail/','')}"
          # Check that result is valid (sometimes url fails to load properly)
          file = File.read "#{ABSTRACTS_OUTPUT_DIR}/#{MS_URL.gsub('://','/').gsub('/','_')}#{link.gsub('/','_')}/results.json"
          json = JSON.parse(file)
          if json['title']['value'][0] # If we captured a title, don't keep trying
            keep_trying = false
          else
            puts "    try again..."
          end
        end
      end
    end

    # Prepare CSV file for final output 
    final_csv = File.join(OUTPUT_DIR, "final_output_#{timestamp}.csv")
    csv = CSV.open(final_csv, "wb")
    csv << ['URL', 'Title', 'Author', 'Abstract']

    # Cycle over abstracts scraped from the abstract links
    Dir.glob("#{ABSTRACTS_OUTPUT_DIR}/https_academic.*") do |abstract|
      puts "\nProcessing scraped data at #{abstract}"
      file = File.read abstract+'/results.json'
      json = JSON.parse(file)

      # Retrieve and process abstract data
      url = abstract[/http.*/].gsub(/http_/,'http://').gsub(/_/, '/')
      title = json['title']['value'][0]
      authors = json['author']['value'].map {|a| a.match(/\(.*\)/) ? nil : a }.compact.join(', ')
      text = json['abstract']['value'][0]

      if !text.empty? && !authors.empty?
        puts "  Abstract available"
        csv << [url, title, authors, text]
      elsif authors.empty?
        puts "  No author(s) available"
      else
        puts "  No abstract available"
      end
    end

    # Calculate execution time 
    puts "\nExecution Time: #{Time.at((Time.now - start_time).round(0)).utc.strftime("%H:%M:%S")}"

    puts "\nFinal output available at #{final_csv}"
  end

end
