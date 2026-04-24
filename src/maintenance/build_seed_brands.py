"""
Generate ``data/brands_seed.csv`` from three sources:

1. Existing hardcoded brand registry in ``src.fetchers.brand_scraper``
2. ``BRAND_CATEGORY`` map in ``src.analysis.competitor``
3. Curated list of top Indian retail brands (below)

Re-run this whenever the curated list is updated. The script is idempotent:
duplicates (by lowercased canonical_name) collapse, aliases merge, and the
earliest-listed category wins when two sources disagree.

Target: 500+ entries across QSR, apparel, jewelry, beauty, electronics,
grocery, healthcare, cafe, and a handful of other verticals.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from src.analysis.competitor import BRAND_CATEGORY  # noqa: E402
from src.fetchers.brand_scraper import BRAND_REGISTRY  # noqa: E402

OUT_PATH = ROOT / "data" / "brands_seed.csv"
GAPS_PATH = ROOT / "data" / "brands_seed_gaps.md"


CURATED_BRANDS: list[tuple[str, list[str], str]] = [
    # --- biryani ---
    ("Biryani By Kilo", ["BBK", "Biryani By Kilo Ltd"], "biryani"),
    ("Behrouz Biryani", ["Behrooz Biryani"], "biryani"),
    ("Paradise Biryani", ["Paradise Food Court", "Paradise"], "biryani"),
    ("Bawarchi", [], "biryani"),
    ("Shah Ghouse", [], "biryani"),
    ("Meridian Restaurant", ["Meridian"], "biryani"),
    ("Biryani Blues", [], "biryani"),
    ("Hyderabad House", [], "biryani"),
    ("Bikkgane Biryani", [], "biryani"),
    ("Biryani Zone", [], "biryani"),
    ("Charcoal Biryani", [], "biryani"),
    ("Mani's Dum Biryani", [], "biryani"),
    ("Biryani Guru", [], "biryani"),
    ("Bhai Veetu", [], "biryani"),
    ("Andhra Bhavan", [], "biryani"),
    ("Pista House", [], "biryani"),

    # --- pizza ---
    ("Dominos Pizza", ["Dominos", "Domino's", "Domino's Pizza"], "pizza"),
    ("Pizza Hut", [], "pizza"),
    ("La Pino'z", ["La Pinoz", "La Pinos"], "pizza"),
    ("Mojo Pizza", ["MOJO Pizza"], "pizza"),
    ("Papa John's", ["Papa Johns"], "pizza"),
    ("Oven Story", ["Oven Story Pizza"], "pizza"),
    ("Smokin' Joes", ["Smokin Joes"], "pizza"),
    ("Sbarro", [], "pizza"),
    ("Chicago Pizza", [], "pizza"),
    ("Pizza Express", [], "pizza"),
    ("California Pizza Kitchen", ["CPK"], "pizza"),
    ("Pizza Metro Pizza", [], "pizza"),
    ("1441 Pizzeria", [], "pizza"),
    ("Joey's Pizza", [], "pizza"),
    ("Slice Of Italy", [], "pizza"),
    ("Go 69 Pizza", [], "pizza"),

    # --- burger ---
    ("McDonald's", ["Mac's", "McD", "Mickey D's", "McDonalds"], "burger"),
    ("Burger King", ["BK"], "burger"),
    ("Wendy's", ["Wendys"], "burger"),
    ("Burger Singh", [], "burger"),
    ("Biggies Burger", [], "burger"),
    ("Jumboking", ["Jumbo King"], "burger"),
    ("Louis Burger", [], "burger"),
    ("The Burger Club", [], "burger"),
    ("Nazeer Foods", ["Nazeer"], "burger"),
    ("Carl's Jr", ["Carls Jr"], "burger"),
    ("Johnny Rockets", [], "burger"),
    ("Fatburger", [], "burger"),
    ("Hard Rock Cafe", [], "burger"),
    ("Smash Burger", [], "burger"),

    # --- chicken_qsr ---
    ("KFC", ["Kentucky Fried Chicken"], "chicken_qsr"),
    ("Popeyes", [], "chicken_qsr"),
    ("Chick Blast", [], "chicken_qsr"),
    ("Al Baik", [], "chicken_qsr"),
    ("FreshMenu", [], "chicken_qsr"),
    ("Zambar", [], "chicken_qsr"),
    ("Chicking", [], "chicken_qsr"),
    ("Charcoal Eats", [], "chicken_qsr"),

    # --- sandwich / subway style ---
    ("Subway", [], "sandwich"),
    ("Quiznos", [], "sandwich"),
    ("Wich Please", [], "sandwich"),
    ("Bombay Sandwich Company", [], "sandwich"),

    # --- chinese ---
    ("Mainland China", ["Mainland Chinese"], "chinese"),
    ("Berco's", ["Bercos"], "chinese"),
    ("China Garden", [], "chinese"),
    ("Yauatcha", [], "chinese"),
    ("Hakkasan", [], "chinese"),
    ("Mad Over Donuts", [], "chinese"),
    ("China Bistro", [], "chinese"),
    ("Pan Asian", [], "chinese"),
    ("China House", [], "chinese"),
    ("Chung Wah", [], "chinese"),
    ("Asia Kitchen", [], "chinese"),
    ("Wang's Kitchen", [], "chinese"),

    # --- south indian ---
    ("Saravana Bhavan", ["Hotel Saravana Bhavan", "Sarvana Bhavan"], "south_indian"),
    ("MTR", ["Mavalli Tiffin Rooms"], "south_indian"),
    ("Murugan Idli Shop", ["Murugan Idli"], "south_indian"),
    ("Adyar Ananda Bhavan", ["A2B"], "south_indian"),
    ("Sangeetha Veg Restaurant", ["Sangeetha"], "south_indian"),
    ("Vidyarthi Bhavan", [], "south_indian"),
    ("CTR", ["Central Tiffin Room"], "south_indian"),
    ("Brahmin's Coffee Bar", [], "south_indian"),
    ("Ratna Cafe", [], "south_indian"),
    ("Komala's", ["Komalas"], "south_indian"),
    ("Naivedyam", [], "south_indian"),
    ("Dakshin Express", [], "south_indian"),

    # --- north indian / indian_qsr ---
    ("Haldiram's", ["Haldirams", "Haldiram"], "indian_qsr"),
    ("Bikanervala", ["Bikano"], "indian_qsr"),
    ("Wow! Momo", ["Wow Momo", "WowMomo"], "indian_qsr"),
    ("Karims", ["Karim's", "Kareem's"], "indian_qsr"),
    ("Moti Mahal", ["Moti Mahal Delux"], "indian_qsr"),
    ("Barbeque Nation", ["BBQ Nation", "Barbecue Nation"], "indian_qsr"),
    ("Punjab Grill", [], "indian_qsr"),
    ("Gazab Indian", [], "indian_qsr"),
    ("Sagar Ratna", [], "indian_qsr"),
    ("Nathu Sweets", [], "indian_qsr"),
    ("Om Sweets", [], "indian_qsr"),
    ("Gopaljee", [], "indian_qsr"),
    ("Bengali Sweet House", [], "indian_qsr"),
    ("Chhappan Bhog", [], "indian_qsr"),
    ("Rajdhani Thali", ["Rajdhani"], "indian_qsr"),

    # --- ice_cream ---
    ("Baskin Robbins", ["Baskin-Robbins", "BR"], "ice_cream"),
    ("Naturals Ice Cream", ["Naturals"], "ice_cream"),
    ("Havmor", [], "ice_cream"),
    ("Amul Ice Cream", ["Amul"], "ice_cream"),
    ("Vadilal", [], "ice_cream"),
    ("Kwality Walls", ["Kwality Wall's"], "ice_cream"),
    ("Mother Dairy Ice Cream", ["Mother Dairy"], "ice_cream"),
    ("Gelato Italiano", [], "ice_cream"),
    ("London Dairy", [], "ice_cream"),
    ("Cream Bell", [], "ice_cream"),
    ("Keventers", [], "ice_cream"),
    ("Frozen Bottle", [], "ice_cream"),
    ("Giani's", ["Gianis"], "ice_cream"),
    ("Nirulas Ice Cream", ["Nirulas"], "ice_cream"),
    ("Cold Stone Creamery", [], "ice_cream"),
    ("Baskin and Robbins", [], "ice_cream"),

    # --- coffee ---
    ("Starbucks", [], "coffee"),
    ("Cafe Coffee Day", ["CCD", "Coffee Day"], "coffee"),
    ("Barista", ["Barista Coffee"], "coffee"),
    ("Blue Tokai", ["Blue Tokai Coffee Roasters"], "coffee"),
    ("Third Wave Coffee", ["Third Wave Coffee Roasters", "TWCR"], "coffee"),
    ("Costa Coffee", ["Costa"], "coffee"),
    ("Araku Coffee", [], "coffee"),
    ("Dunkin Donuts", ["Dunkin'", "Dunkin"], "coffee"),
    ("Tim Hortons", ["Tim Horton's"], "coffee"),
    ("Indian Coffee House", ["ICH"], "coffee"),
    ("Di Bella Coffee", [], "coffee"),
    ("Roastery Coffee House", ["Roastery"], "coffee"),
    ("%Arabica", ["Arabica"], "coffee"),
    ("Subko", ["Subko Coffee"], "coffee"),
    ("Slay Coffee", ["Slay"], "coffee"),
    ("Davidoff Cafe", [], "coffee"),

    # --- tea / chai ---
    ("Chaayos", [], "tea"),
    ("Chai Point", [], "tea"),
    ("Chai Sutta Bar", ["CSB"], "tea"),
    ("MBA Chai Wala", [], "tea"),
    ("Tea Trails", [], "tea"),
    ("Wagh Bakri Tea Lounge", ["Wagh Bakri"], "tea"),
    ("Tea Villa Cafe", [], "tea"),
    ("Indian Tea House", [], "tea"),
    ("TPOT", [], "tea"),
    ("Tapri Central", [], "tea"),
    ("Cha Bar", [], "tea"),

    # --- juice / beverages ---
    ("Keventers Milkshake", [], "beverage"),
    ("Frozen Bottle Shake", [], "beverage"),
    ("Boost Juice", [], "beverage"),
    ("The Juice Junction", [], "beverage"),
    ("Joos Box", [], "beverage"),
    ("Fruit Bae", [], "beverage"),

    # --- bakery ---
    ("Theobroma", [], "bakery"),
    ("Monginis", ["Mongini's"], "bakery"),
    ("Mad Over Donuts India", ["Mad Over Donuts"], "bakery"),
    ("Dunkin' Donuts India", [], "bakery"),
    ("Wenger's", ["Wengers"], "bakery"),
    ("Defence Bakery", [], "bakery"),
    ("L'Opera", ["LOpera", "L Opera"], "bakery"),
    ("Cakezone", [], "bakery"),
    ("FB Cakes", [], "bakery"),
    ("Winni Cakes", ["Winni"], "bakery"),
    ("The French Loaf", [], "bakery"),
    ("Karachi Bakery", [], "bakery"),
    ("Daily Bread", [], "bakery"),
    ("Candies", [], "bakery"),
    ("Bakingo", [], "bakery"),
    ("Kenny Rogers Roasters", [], "bakery"),
    ("Donut Baker", [], "bakery"),

    # --- apparel (mass) ---
    ("Zara", [], "apparel"),
    ("H&M", ["H and M"], "apparel"),
    ("Uniqlo", [], "apparel"),
    ("Max Fashion", ["Max"], "apparel"),
    ("Pantaloons", [], "apparel"),
    ("Westside", [], "apparel"),
    ("Lifestyle", ["Lifestyle Stores"], "apparel"),
    ("Shoppers Stop", ["Shopper's Stop"], "apparel"),
    ("Central", ["Central Mall"], "apparel"),
    ("Reliance Trends", [], "apparel"),
    ("V Mart", ["V-Mart", "Vmart"], "apparel"),
    ("Style Bazaar", ["Style Bazar"], "apparel"),
    ("Trends Footwear", [], "apparel"),
    ("Brand Factory", [], "apparel"),
    ("Marks and Spencer", ["M&S", "Marks & Spencer"], "apparel"),
    ("Tommy Hilfiger", [], "apparel"),
    ("Levi's", ["Levis"], "apparel"),
    ("Jack & Jones", ["Jack and Jones"], "apparel"),
    ("Only", [], "apparel"),
    ("Vero Moda", [], "apparel"),
    ("US Polo Assn", ["U.S. Polo Assn.", "US Polo"], "apparel"),
    ("Allen Solly", [], "apparel"),
    ("Peter England", [], "apparel"),
    ("Van Heusen", [], "apparel"),
    ("Louis Philippe", [], "apparel"),
    ("Raymond", [], "apparel"),
    ("Manyavar", [], "apparel"),
    ("Mohey", [], "apparel"),
    ("Killer Jeans", ["Killer"], "apparel"),
    ("Being Human", [], "apparel"),
    ("Cantabil", [], "apparel"),
    ("Mufti", [], "apparel"),
    ("Blackberrys", ["Blackberry's"], "apparel"),
    ("Colorplus", ["ColorPlus"], "apparel"),
    ("Park Avenue", [], "apparel"),
    ("Wrangler", [], "apparel"),
    ("Lee", [], "apparel"),
    ("Pepe Jeans", [], "apparel"),
    ("Benetton", ["United Colors of Benetton", "UCB"], "apparel"),
    ("Flying Machine", [], "apparel"),
    ("Arrow", [], "apparel"),
    ("Indian Terrain", [], "apparel"),

    # --- ethnic wear ---
    ("FabIndia", ["Fab India"], "ethnic_wear"),
    ("BIBA", ["Biba"], "ethnic_wear"),
    ("W for Woman", ["W"], "ethnic_wear"),
    ("Aurelia", [], "ethnic_wear"),
    ("Global Desi", [], "ethnic_wear"),
    ("Soch", [], "ethnic_wear"),
    ("Kalki Fashion", ["Kalki"], "ethnic_wear"),
    ("Sabyasachi", [], "ethnic_wear"),
    ("Anita Dongre", [], "ethnic_wear"),
    ("Ritu Kumar", [], "ethnic_wear"),
    ("Neeru's", ["Neerus"], "ethnic_wear"),
    ("Kalaniketan", [], "ethnic_wear"),
    ("Meena Bazaar", [], "ethnic_wear"),
    ("Nalli Silks", ["Nalli"], "ethnic_wear"),
    ("Pothys", [], "ethnic_wear"),
    ("Chennai Silks", [], "ethnic_wear"),
    ("RmKV", ["RMKV"], "ethnic_wear"),
    ("Kanchipuram Silks", [], "ethnic_wear"),
    ("Sundari Silks", [], "ethnic_wear"),
    ("Lovely Wedding Mall", [], "ethnic_wear"),

    # --- kidswear ---
    ("FirstCry", ["First Cry", "Firstcry"], "kidswear"),
    ("Gini & Jony", ["Gini and Jony"], "kidswear"),
    ("Mothercare", [], "kidswear"),
    ("Hopscotch", [], "kidswear"),
    ("Kids Kemp", [], "kidswear"),
    ("Hamleys", [], "kidswear"),

    # --- footwear ---
    ("Bata", [], "footwear"),
    ("Metro Shoes", ["Metro"], "footwear"),
    ("Liberty Shoes", ["Liberty"], "footwear"),
    ("Relaxo", ["Relaxo Footwear"], "footwear"),
    ("Woodland", [], "footwear"),
    ("Red Tape", [], "footwear"),
    ("Crocs", [], "footwear"),
    ("Puma", [], "footwear"),
    ("Nike", [], "footwear"),
    ("Adidas", [], "footwear"),
    ("Reebok", [], "footwear"),
    ("Skechers", [], "footwear"),
    ("Clarks", [], "footwear"),
    ("Mochi Shoes", ["Mochi"], "footwear"),
    ("Khadim's", ["Khadims"], "footwear"),
    ("Campus Shoes", ["Campus"], "footwear"),
    ("Paragon", [], "footwear"),

    # --- accessories / leather ---
    ("Da Milano", [], "leather"),
    ("Hidesign", [], "leather"),
    ("Fossil", [], "accessories"),
    ("Timex", [], "accessories"),
    ("Daniel Wellington", [], "accessories"),
    ("Tommy Hilfiger Accessories", [], "accessories"),
    ("Titan Watches", ["Titan"], "accessories"),
    ("Helios Watches", ["Helios"], "accessories"),
    ("Fastrack", [], "accessories"),
    ("Ethos Watches", ["Ethos"], "accessories"),
    ("Kama Ayurveda", [], "accessories"),
    ("Hidesign Bags", [], "accessories"),

    # --- jewelry ---
    ("Tanishq", [], "jewellery"),
    ("Malabar Gold", ["Malabar Gold and Diamonds"], "jewellery"),
    ("Kalyan Jewellers", ["Kalyan"], "jewellery"),
    ("PC Jeweller", ["PCJ"], "jewellery"),
    ("CaratLane", ["Caratlane"], "jewellery"),
    ("Joyalukkas", ["Joy Alukkas"], "jewellery"),
    ("Senco Gold", ["Senco"], "jewellery"),
    ("Tribhovandas Bhimji Zaveri", ["TBZ"], "jewellery"),
    ("GRT Jewellers", ["GRT"], "jewellery"),
    ("Khazana Jewellery", ["Khazana"], "jewellery"),
    ("Bhima Jewellers", ["Bhima"], "jewellery"),
    ("Mia by Tanishq", ["Mia"], "jewellery"),
    ("Reliance Jewels", [], "jewellery"),
    ("D'Damas", ["Ddamas", "D Damas"], "jewellery"),
    ("Lalithaa Jewellery", ["Lalithaa"], "jewellery"),
    ("Waman Hari Pethe", ["WHP", "WHP Jewellers"], "jewellery"),
    ("PP Jewellers", [], "jewellery"),
    ("BlueStone", ["Blue Stone"], "jewellery"),
    ("Candere", [], "jewellery"),
    ("Vaibhav Global", [], "jewellery"),
    ("PNG Jewellers", ["PNG"], "jewellery"),

    # --- eyewear ---
    ("Lenskart", [], "eyewear"),
    ("Titan Eye Plus", ["Titan EyePlus", "Titan Eye+"], "eyewear"),
    ("Specsmakers", [], "eyewear"),
    ("GKB Opticals", ["GKB"], "eyewear"),
    ("Vision Express", [], "eyewear"),
    ("Coolwinks", [], "eyewear"),

    # --- beauty / cosmetics ---
    ("Nykaa", [], "beauty"),
    ("Sephora", [], "beauty"),
    ("MAC Cosmetics", ["MAC"], "beauty"),
    ("The Body Shop", [], "beauty"),
    ("Forest Essentials", [], "beauty"),
    ("Kama Ayurveda Beauty", ["Kama Ayurveda"], "beauty"),
    ("Lakme Salon", ["Lakme"], "beauty"),
    ("MyGlamm", [], "beauty"),
    ("Innisfree", [], "beauty"),
    ("The Face Shop", [], "beauty"),
    ("Plum Goodness", ["Plum"], "beauty"),
    ("Biotique", [], "beauty"),
    ("Just Herbs", [], "beauty"),
    ("Himalaya Herbals", ["Himalaya"], "beauty"),
    ("Patanjali", [], "beauty"),
    ("Sugar Cosmetics", ["Sugar"], "beauty"),
    ("Mamaearth", [], "beauty"),
    ("WOW Skin Science", ["WOW"], "beauty"),
    ("Minimalist", [], "beauty"),
    ("Dot & Key", [], "beauty"),

    # --- salon ---
    ("Naturals Salon", ["Naturals"], "salon"),
    ("Jawed Habib", ["Habib's"], "salon"),
    ("VLCC", [], "salon"),
    ("YLG Salon", ["YLG"], "salon"),
    ("BBLUNT", [], "salon"),
    ("Enrich Salon", ["Enrich"], "salon"),
    ("Geetanjali Salon", [], "salon"),
    ("Looks Salon", [], "salon"),
    ("Green Trends", [], "salon"),
    ("TruefittAndHill", ["Truefitt and Hill"], "salon"),
    ("Strands Salon", [], "salon"),
    ("Affinity Salon", [], "salon"),
    ("Toni & Guy", ["Toni and Guy"], "salon"),
    ("Salon Stories", [], "salon"),

    # --- electronics ---
    ("Croma", [], "electronics"),
    ("Reliance Digital", [], "electronics"),
    ("Vijay Sales", [], "electronics"),
    ("Sangeetha Mobiles", ["Sangeetha"], "electronics"),
    ("Poorvika Mobiles", ["Poorvika"], "electronics"),
    ("Lot Mobiles", [], "electronics"),
    ("Big C Mobiles", ["Big C"], "electronics"),
    ("Apple Store", ["Apple"], "electronics"),
    ("Samsung Experience", ["Samsung"], "electronics"),
    ("Mi Home", ["Xiaomi", "Mi"], "electronics"),
    ("OnePlus Experience", ["OnePlus"], "electronics"),
    ("Oppo Experience", ["Oppo"], "electronics"),
    ("Vivo Experience", ["Vivo"], "electronics"),
    ("Realme", [], "electronics"),
    ("Bajaj Electronics", [], "electronics"),

    # --- grocery / supermarkets ---
    ("Reliance Fresh", [], "grocery"),
    ("Reliance Smart", [], "grocery"),
    ("Big Bazaar", [], "grocery"),
    ("DMart", ["D-Mart", "D Mart", "Avenue Supermarts"], "grocery"),
    ("More Supermarket", ["More"], "grocery"),
    ("Spencer's Retail", ["Spencers", "Spencer's"], "grocery"),
    ("Star Bazaar", [], "grocery"),
    ("Nature's Basket", ["Natures Basket"], "grocery"),
    ("Le Marche", [], "grocery"),
    ("Ratnadeep Supermarket", ["Ratnadeep"], "grocery"),
    ("Namdhari's Fresh", ["Namdharis"], "grocery"),
    ("Heritage Fresh", ["Heritage"], "grocery"),
    ("EasyDay", ["Easy Day"], "grocery"),
    ("FreshPik", [], "grocery"),
    ("Vishal Mega Mart", ["Vishal"], "grocery"),
    ("Hypercity", [], "grocery"),

    # --- specialty food / confectionery ---
    ("Nathu's Sweets", ["Nathus"], "sweet_shop"),
    ("Haldiram's Nagpur", ["Haldiram Nagpur"], "sweet_shop"),
    ("Agarwal Sweet Corner", ["Agarwal Sweets"], "sweet_shop"),
    ("Brijwasi Royal Sweets", ["Brijwasi"], "sweet_shop"),
    ("Kanha Sweets", [], "sweet_shop"),
    ("Anand Sweets", [], "sweet_shop"),
    ("Tewari Brothers", [], "sweet_shop"),
    ("KC Das", [], "sweet_shop"),
    ("Balaram Mullick", [], "sweet_shop"),
    ("Ganguram", [], "sweet_shop"),
    ("Mithaas", [], "sweet_shop"),
    ("Harnarains", [], "sweet_shop"),
    ("Om Sweets and Snacks", [], "sweet_shop"),

    # --- pharmacy ---
    ("Apollo Pharmacy", ["Apollo"], "pharmacy"),
    ("MedPlus", ["Med Plus"], "pharmacy"),
    ("Wellness Forever", [], "pharmacy"),
    ("Netmeds", [], "pharmacy"),
    ("1MG Store", ["1MG", "Tata 1mg"], "pharmacy"),
    ("PharmEasy", [], "pharmacy"),
    ("Guardian Pharmacy", ["Guardian"], "pharmacy"),
    ("Frank Ross Pharmacy", ["Frank Ross"], "pharmacy"),
    ("Religare Wellness", [], "pharmacy"),
    ("Hetero Med Plus", [], "pharmacy"),

    # --- diagnostics ---
    ("Dr. Lal PathLabs", ["Dr Lal PathLabs", "Lal PathLabs"], "diagnostic"),
    ("Thyrocare", [], "diagnostic"),
    ("SRL Diagnostics", ["SRL"], "diagnostic"),
    ("Metropolis Healthcare", ["Metropolis"], "diagnostic"),
    ("Vijaya Diagnostics", [], "diagnostic"),
    ("Apollo Diagnostics", [], "diagnostic"),
    ("Suburban Diagnostics", [], "diagnostic"),

    # --- dental / clinics ---
    ("Clove Dental", ["Clove"], "dental"),
    ("Apollo White Dental", [], "dental"),
    ("Dentzz Dental", [], "dental"),
    ("Partha Dental", [], "dental"),
    ("FMS Dental", [], "dental"),

    # --- hospital / clinic chains ---
    ("Apollo Hospital", ["Apollo Hospitals"], "hospital"),
    ("Fortis Hospital", ["Fortis"], "hospital"),
    ("Max Healthcare", ["Max Hospital"], "hospital"),
    ("Manipal Hospital", ["Manipal Hospitals"], "hospital"),
    ("Narayana Health", [], "hospital"),
    ("Medanta", [], "hospital"),
    ("AIIMS", [], "hospital"),
    ("Cloudnine Hospital", ["Cloudnine"], "hospital"),
    ("Rainbow Hospitals", [], "hospital"),
    ("KIMS Hospital", [], "hospital"),

    # --- fitness ---
    ("Gold's Gym", ["Golds Gym"], "gym"),
    ("Cult Fit", ["Cultfit", "Cult.fit"], "gym"),
    ("Anytime Fitness", [], "gym"),
    ("Snap Fitness", [], "gym"),
    ("Talwalkars", [], "gym"),
    ("Fitness First", [], "gym"),
    ("Gym Pro", [], "gym"),

    # --- pet ---
    ("Heads Up For Tails", ["HUFT"], "pet_store"),
    ("Just Dogs", [], "pet_store"),
    ("Glenand", [], "pet_store"),
    ("Barks n Wags", [], "pet_store"),

    # --- home furnishings ---
    ("Home Centre", ["HomeCentre"], "home_furnishing"),
    ("Pepperfry", [], "home_furnishing"),
    ("Urban Ladder", [], "home_furnishing"),
    ("IKEA", [], "home_furnishing"),
    ("@Home", ["At Home"], "home_furnishing"),
    ("HomeStop", [], "home_furnishing"),
    ("Fabindia Home", [], "home_furnishing"),
    ("Good Earth", [], "home_furnishing"),
    ("Nilkamal", [], "home_furnishing"),

    # --- books / stationery ---
    ("Crossword Bookstores", ["Crossword"], "bookstore"),
    ("Oxford Bookstore", ["Oxford Books"], "bookstore"),
    ("Higginbothams", [], "bookstore"),
    ("Landmark", ["Landmark Bookstore"], "bookstore"),
    ("Sapna Book House", ["Sapna"], "bookstore"),
    ("Starmark", [], "bookstore"),
    ("Kitab Khana", [], "bookstore"),
    ("Walden Bookstore", ["Walden"], "bookstore"),

    # --- toys / hobby ---
    ("Toys R Us", ["ToysRUs"], "toy_store"),
    ("Funskool", [], "toy_store"),
    ("Imagica Toys", [], "toy_store"),

    # --- gifting ---
    ("Archies", [], "gift_shop"),
    ("Ferns N Petals", ["FNP"], "gift_shop"),
    ("Hallmark", [], "gift_shop"),

    # --- automotive services ---
    ("Maruti Suzuki Service", ["Maruti Suzuki Arena"], "automotive"),
    ("Hyundai Service", ["Hyundai"], "automotive"),
    ("Honda Cars", [], "automotive"),
    ("Tata Motors Showroom", ["Tata Motors"], "automotive"),
    ("Mahindra Dealership", ["Mahindra"], "automotive"),
    ("Hero MotoCorp", ["Hero"], "automotive"),
    ("Bajaj Auto", [], "automotive"),
    ("Royal Enfield", [], "automotive"),

    # --- hotels / stays ---
    ("Taj Hotels", ["Taj"], "hotel"),
    ("ITC Hotels", ["ITC"], "hotel"),
    ("Oberoi Hotels", ["Oberoi"], "hotel"),
    ("Marriott", [], "hotel"),
    ("Hyatt", [], "hotel"),
    ("Radisson", [], "hotel"),
    ("Lemon Tree Hotels", ["Lemon Tree"], "hotel"),
    ("OYO Rooms", ["OYO"], "hotel"),
    ("Treebo Hotels", ["Treebo"], "hotel"),
    ("FabHotels", [], "hotel"),
    ("ibis", [], "hotel"),

    # --- logistics / courier ---
    ("Blue Dart", [], "logistics"),
    ("DTDC", [], "logistics"),
    ("FedEx", [], "logistics"),
    ("DHL", [], "logistics"),
    ("India Post", [], "logistics"),

    # --- banks / atm ---
    ("HDFC Bank", ["HDFC"], "bank"),
    ("ICICI Bank", ["ICICI"], "bank"),
    ("State Bank of India", ["SBI"], "bank"),
    ("Axis Bank", ["Axis"], "bank"),
    ("Kotak Mahindra Bank", ["Kotak"], "bank"),
    ("IndusInd Bank", ["IndusInd"], "bank"),
    ("Yes Bank", [], "bank"),
    ("Punjab National Bank", ["PNB"], "bank"),
    ("Bank of Baroda", ["BoB"], "bank"),
    ("Canara Bank", ["Canara"], "bank"),
    ("Union Bank of India", ["Union Bank"], "bank"),
    ("Bajaj Finserv", [], "bank"),

    # --- mobile / network retail ---
    ("Jio Store", ["Reliance Jio", "Jio"], "telecom"),
    ("Airtel Store", ["Airtel"], "telecom"),
    ("Vi Store", ["Vodafone Idea", "Vi"], "telecom"),
    ("BSNL", [], "telecom"),

    # --- stationery / office ---
    ("Staples India", ["Staples"], "stationery"),
    ("Office Depot", [], "stationery"),

    # --- cloud kitchens / delivery brands ---
    ("Faasos", [], "cloud_kitchen"),
    ("Box8", ["Box 8"], "cloud_kitchen"),
    ("Rebel Foods", [], "cloud_kitchen"),
    ("Sleepy Owl Coffee", ["Sleepy Owl"], "cloud_kitchen"),
    ("The Good Bowl", [], "cloud_kitchen"),
    ("EatSure", [], "cloud_kitchen"),

    # --- additional QSR / casual dining fill ---
    ("The Big Chill Cafe", ["Big Chill"], "cafe"),
    ("Social", ["The Social"], "cafe"),
    ("Hard Rock Cafe India", ["Hard Rock"], "cafe"),
    ("Mocha", [], "cafe"),
    ("Smoke House Deli", ["Smoke House"], "cafe"),
    ("The Beer Cafe", [], "cafe"),
    ("Indigo Deli", [], "cafe"),
    ("Olive Bistro", [], "cafe"),
    ("Cafe Delhi Heights", ["Delhi Heights"], "cafe"),
    ("The Yellow Chilli", ["Yellow Chilli"], "cafe"),
    ("Asia 7", [], "cafe"),
    ("Farzi Cafe", ["Farzi"], "cafe"),
    ("Masala Library", [], "cafe"),
    ("Bombay Canteen", [], "cafe"),
    ("SodaBottleOpenerWala", ["Soda Bottle Opener Wala"], "cafe"),
    ("The Big Pitcher", [], "cafe"),
    ("Toit Brewpub", ["Toit"], "cafe"),
    ("Arbor Brewing", [], "cafe"),
    ("Prost Brewpub", ["Prost"], "cafe"),
    ("Beer Cafe", [], "cafe"),
    ("Imperfecto", [], "cafe"),

    # --- additional apparel / fashion fill ---
    ("Puma Stores", [], "apparel"),
    ("Under Armour", [], "apparel"),
    ("Columbia Sportswear", ["Columbia"], "apparel"),
    ("North Face", [], "apparel"),
    ("Wildcraft", [], "apparel"),
    ("Decathlon", [], "apparel"),
    ("Forever 21", [], "apparel"),
    ("Mango", [], "apparel"),
    ("ASICS", [], "apparel"),
    ("Calvin Klein", ["CK"], "apparel"),
    ("Gap", [], "apparel"),
    ("Banana Republic", [], "apparel"),
    ("Steve Madden", [], "apparel"),

    # --- additional jewelry / luxury ---
    ("Forevermark", [], "jewellery"),
    ("Swarovski", [], "jewellery"),
    ("Gitanjali Jewels", ["Gitanjali"], "jewellery"),
    ("Orra Fine Jewellery", ["Orra"], "jewellery"),
    ("Thangamayil Jewellery", ["Thangamayil"], "jewellery"),
    ("VBJ Jewellers", ["VBJ"], "jewellery"),

    # --- more beauty / grooming ---
    ("Bath & Body Works", ["Bath and Body Works"], "beauty"),
    ("L'Occitane", ["LOccitane", "L Occitane"], "beauty"),
    ("Kiehl's", ["Kiehls"], "beauty"),
    ("Bobbi Brown", [], "beauty"),
    ("Clinique", [], "beauty"),
    ("Estee Lauder", ["Estée Lauder"], "beauty"),
    ("Charlotte Tilbury", [], "beauty"),
    ("Neemli Naturals", [], "beauty"),

    # --- appliance / hardware ---
    ("Bajaj Electricals Gallery", ["Bajaj Electricals"], "appliance"),
    ("Havells Galaxy", ["Havells"], "appliance"),
    ("Philips Experience Store", ["Philips"], "appliance"),
    ("LG Best Shop", ["LG"], "appliance"),
    ("Sony Center", ["Sony"], "appliance"),

    # --- more grocery / fresh ---
    ("Gourmet Garden", [], "grocery"),
    ("Modern Bazaar", [], "grocery"),
    ("SPAR Hypermarket", ["SPAR"], "grocery"),
    ("Foodhall", [], "grocery"),
]


def _load_hardcoded_brands() -> list[tuple[str, list[str], str]]:
    """Pull brand names from the Playwright registry; categories inferred where known."""
    known_cats = {
        "Dominos Pizza": "pizza",
        "McDonald's": "burger",
        "Starbucks": "coffee",
        "Da Milano": "leather",
        "Nykaa": "beauty",
        "Tanishq": "jewellery",
        "Lenskart": "eyewear",
    }
    out: list[tuple[str, list[str], str]] = []
    for name in BRAND_REGISTRY.keys():
        out.append((name, [], known_cats.get(name, "")))
    return out


def build_seed_rows() -> list[dict[str, str]]:
    """Merge the three sources. Dedupe by lowercased canonical_name."""
    rows: dict[str, dict[str, object]] = {}

    def _insert(name: str, aliases: list[str], category: str, source: str) -> None:
        name = (name or "").strip()
        if not name:
            return
        key = name.lower()
        if key in rows:
            existing = rows[key]
            merged_aliases = set(existing["aliases"]) | {a.strip() for a in aliases if a.strip()}
            existing["aliases"] = sorted(merged_aliases)
            if not existing["category"] and category:
                existing["category"] = category
            return
        rows[key] = {
            "canonical_name": name,
            "aliases": sorted({a.strip() for a in aliases if a.strip()}),
            "category": category or "",
            "source": source,
        }

    for name, aliases, category in _load_hardcoded_brands():
        _insert(name, aliases, category, "seed")

    for name, cat in BRAND_CATEGORY.items():
        _insert(name, [], cat, "seed")

    for name, aliases, category in CURATED_BRANDS:
        _insert(name, aliases, category, "seed")

    return [
        {
            "canonical_name": r["canonical_name"],
            "aliases": ";".join(r["aliases"]) if r["aliases"] else "",
            "category": r["category"],
            "source": r["source"],
        }
        for r in rows.values()
    ]


def write_seed_csv(rows: list[dict[str, str]], path: Path = OUT_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["canonical_name", "aliases", "category", "source"]
        )
        writer.writeheader()
        for r in sorted(rows, key=lambda x: x["canonical_name"].lower()):
            writer.writerow(r)


def write_gaps_if_short(rows: list[dict[str, str]], target: int = 500) -> None:
    if len(rows) >= target:
        if GAPS_PATH.exists():
            GAPS_PATH.unlink()
        return
    missing = target - len(rows)
    GAPS_PATH.parent.mkdir(parents=True, exist_ok=True)
    GAPS_PATH.write_text(
        f"# Brand seed gaps\n\n"
        f"Seed dataset has {len(rows)} entries (target: {target}).\n"
        f"Short by {missing} brands. Add more curated entries to\n"
        f"`src/scripts/build_seed_brands.py::CURATED_BRANDS` under the\n"
        f"category blocks already scaffolded (biryani, pizza, apparel, etc.).\n"
    )


def main() -> int:
    rows = build_seed_rows()
    write_seed_csv(rows)
    write_gaps_if_short(rows)
    print(f"Wrote {len(rows)} brands to {OUT_PATH.relative_to(ROOT)}")
    uncategorized = sum(1 for r in rows if not r["category"])
    if uncategorized:
        print(f"Note: {uncategorized} entries have no category assigned.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
