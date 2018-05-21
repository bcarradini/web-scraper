require 'json'
require 'csv'

namespace :scrape do
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
end
