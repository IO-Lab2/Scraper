# Punktoza Scraper

This project is designed to scrape journal publications data from punktoza.pl using Scrapy with the Playwright plugin to handle JavaScript-heavy websites. The scraped data is then processed and updated into a PostgreSQL database. The project retrieves information such as journal name, impact factor, publisher, and journal type.
## Features

- Scraping: Uses Scrapy with the Playwright middleware to scrape data from JavaScript-rendered tables.
- Pagination Handling: Automatically handles pagination and continues scraping until all pages are processed.
- Database Integration: Updates journal data in a PostgreSQL database. Only journals already in the database are processed.
- Error Handling: Proper error handling to skip entries that fail during processing.
    Filtering: Optionally filters out journals based on a list in a text file or database entries.

## Project Structure

    punktoza/
    │
    ├── punktoza/spiders/                  
    │   └── punktoza_spider.py            # Main spider responsible for data scraping
    │
    ├── punktoza/items.py                 # Fields from the database
    ├── punktoza/pipelines.py             # Filtering and saving publications to the database
    ├── punktoza/settings.py              # Change scraper's settings
    ├── example_output.json               # Example json output
    ├── full_journal_list.json
    ├── journal_names.txt                 # Add names of journals to filter out (only if using .txt mode)
    ├── .env                              # Store your database connection data
    ├── .gitignore
    ├── scrapy.cfg                        # Configure spider launch options
    └── requirements.txt                  # Install before using

## Prerequisites

Before running the spider, ensure that you have the following:

    Python 3.7+
    PostgreSQL instance
    Required Python dependencies from requirement.txt

## Installing Dependencies

First, install the required dependencies:
```bash
pip install -r requirements.txt
```
<b>You will also need to create an .env file with the following details for connecting to your PostgreSQL database</b>:

    PGHOST=your_host
    PGUSERSCRAPER=your_scraper_user
    PGPASSWORDSCRAPER=your_scraper_password
    PGPORT=your_database_port
    PGDATABASE=your_database_name

## Database Setup

Make sure your PostgreSQL database has the publications table with the following fields:

    CREATE TABLE publications (
        journal VARCHAR(255),
        journal_impact_factor FLOAT,
        publisher VARCHAR(255),
        journal_type VARCHAR(100)
    );

## Running the Spider

To start the scraping process, run the following command:

    scrapy crawl punktoza_spider

This will initiate the spider, which will scrape journal data from punktoza.pl, process it, and update the corresponding records in the PostgreSQL database.

## Example Output

The following is a sample of the JSON output that the spider generates when scraping data from `punktoza.pl`:

```json
[
  {
    "name": "Academy of Management Annals",
    "if_points": "14.3",
    "publisher": "Academy of Management",
    "journal_type": "artykuł"
  },
  {
    "name": "Academy of Management Journal",
    "if_points": "9.5",
    "publisher": "Academy of Management",
    "journal_type": "artykuł"
  },
  {
    "name": "Academy of Management Review",
    "if_points": "19.3",
    "publisher": "Academy of Management",
    "journal_type": "artykuł"
  }
]
```

## Item Pipeline and Filtering

The project includes two pipelines:

- NameFilterPipeline: Filters out journals that are not found in the database (ensuring that only known journals are processed).
- PunktozaPipeline: Processes the scraped items and updates the publications table in PostgreSQL with the scraped data (journal name, impact factor, publisher, and journal type).

## Filtering by Journal Names from txt (Optional)

If you want to filter the journals based on a predefined list (e.g., from a text file), you can uncomment the relevant section in NameFilterPipeline:

```python
journal_names_path = os.path.join(os.path.dirname(__file__), '..', 'journal_names.txt')
with open(journal_names_path, 'r') as file:
    lines = file.readlines()
self.journal_array = [line.strip() for line in lines]
```

## Updating Data

The PunktozaPipeline updates the PostgreSQL publications table for each journal, setting the following fields:

    journal_impact_factor
    publisher
    journal_type

If a journal is not found in the database, the spider will skip it and log the issue.
Configuration

The following settings can be configured in settings.py:

    Concurrency: You can adjust the number of concurrent requests to control the scraping speed.
        CONCURRENT_REQUESTS

    Logging Level: By default, logs are set to show only errors.
        LOG_LEVEL = 'ERROR'

    Playwright Options: Configure Playwright options for headless scraping.
        PLAYWRIGHT_LAUNCH_OPTIONS

    Item Pipelines: Configure which pipelines should be used in the project.
        ITEM_PIPELINES = { ... }

## For more information on Scrapy, Playwright, and database handling, consult the official documentation:

[Scrapy Documentation](https://docs.scrapy.org/en/latest/)\
[Playwright for Python](https://playwright.dev/python/docs/api/class-playwright)\
[PostgreSQL Documentation](https://www.postgresql.org/docs/)
