# web-scraper

This tool utilizes `quickscrape` to scrape data from various web sites; e.g., abstracts from Business and Professional Communication Quarterly (BPCQ).

## PREREQUISITES

- Ruby 2.3.1
- Node.js
- NPM

## INSTALLATION

(1) Install quickscrape via NPM. If permission is denied ("EACCES: permission denied") do not use sudo. Instead, correct your NPM permissions (https://docs.npmjs.com/getting-started/fixing-npm-permissions) and try again.

```
npm install --global quickscrape
```

(2) Install tiny-jsonrpc dependency for quickscrape (this appears to have been left out of the npm installation process for quick scrape).

```
cd /usr/local/lib/node_modules/quickscrape/node_modules/spooky/
npm install tiny-jsonrpc
```

(3) Install all gems in Gemfile

```
gem install bundler
bundle install
```

## USAGE

To scrape title, author, and abstract from BPCQ Volume 57 Issue 3 through Volume 80 Issue 3:

```
rake scrape:bpcq
```

See logs/bpcq/ for debug output.

## NOTES

You can retrieve a variety of pre-cooked scraper definitions for quickscrape:
    git clone https://github.com/ContentMine/journal-scrapers.git

## LICENSE

This tool is not licensed, but quickscrape is. Refer to https://github.com/ContentMine/quickscrape#license.
