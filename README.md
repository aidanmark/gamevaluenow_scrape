# gamevaluenow_scrape
Scrape of the most expensive assets traded on eBay, according to a game pricing website.


I wrote this code with the intention of finding all of the video games that are trading for the highest prices on eBay.
I completed this by scraping a website that displays all video games and their historical eBay sales.

My pilot function returns all video games that are sold for more than or equal to $1000.
It contains three nested functions: search_assets, get_data, and parse_data.

search_assets: when a video game’s game_id and platform_id is imputed, its html link is called
               and all of the historical sales data of the specified game will be returned.

get_data: retrieves website data when a link is inputted.

parse_data: iterates through all companies, platforms, and games.
            For each game, if its new price is greater than or equal to $1000, then search_assets will run, returning all of its eBay listings.
            All listings with a buy price greater than or equal to $1000 will be added to a dataframe that is returned.
