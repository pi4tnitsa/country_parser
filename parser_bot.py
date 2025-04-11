import asyncio
import os
import json
from datetime import datetime, timedelta
import pytz
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError, FloodWaitError, RPCError
from telethon.tl.types import PeerChannel
import sys
import logging
import traceback
from config import BOT_TOKEN, PASSWORD, DB_NAME, DB_USER, DB_PASSWORD, DB_HOST, DB_PORT

# Log files
LOG_FILE = "bot.log"
USER_LOG_FILE = "user_actions.log"
MAX_LOG_LINES = 10000

# Configure logging for bot operations
def trim_log_file(log_file):
    """Trim log file to MAX_LOG_LINES, removing oldest entries."""
    try:
        if os.path.exists(log_file):
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            if len(lines) > MAX_LOG_LINES:
                with open(log_file, 'w', encoding='utf-8') as f:
                    f.writelines(lines[-MAX_LOG_LINES:])
                logging.info(f"Log file {log_file} trimmed to {MAX_LOG_LINES} lines")
    except Exception as e:
        logging.error(f"Error trimming log file {log_file}: {e}")

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s - [%(filename)s:%(lineno)d]',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# Separate logger for user actions
user_logger = logging.getLogger('user_actions')
user_handler = logging.FileHandler(USER_LOG_FILE)
user_handler.setFormatter(logging.Formatter('%(asctime)s - UserID:%(user_id)s - %(message)s'))
user_logger.addHandler(user_handler)
user_logger.setLevel(logging.INFO)

telethon_logger = logging.getLogger('telethon')
telethon_logger.setLevel(logging.ERROR)

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

active_clients = {}

class ExportStates(StatesGroup):
    waiting_for_country = State()
    waiting_for_period = State()
    waiting_for_format = State()
    waiting_for_custom_month = State()

class ManageCountryStates(StatesGroup):
    waiting_for_password = State()
    waiting_for_action = State()
    waiting_for_country_name = State()
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_phone_number = State()
    waiting_for_verification_code = State()
    waiting_for_country_to_remove = State()

class AdminStates(StatesGroup):
    waiting_for_admin_password = State()
    waiting_for_log_password = State()
    waiting_for_user_log_password = State()
    waiting_for_dump_password = State()
    waiting_for_user_id = State()
    waiting_for_user_to_remove = State()

def connect_to_db():
    try:
        logging.info("Connecting to database...")
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        logging.info("Database connection successful")
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        logging.error(traceback.format_exc())
        raise

def init_db():
    try:
        logging.info("Initializing database...")
        conn = connect_to_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)  # Используем RealDictCursor
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            channel_id BIGINT,
            channel_name TEXT NOT NULL,
            country TEXT NOT NULL,
            UNIQUE(channel_id, country)
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS country_configs (
            id SERIAL PRIMARY KEY,
            country_name TEXT NOT NULL UNIQUE,
            api_id TEXT NOT NULL,
            api_hash TEXT NOT NULL,
            phone_number TEXT NOT NULL,
            session_name TEXT NOT NULL
        )
        ''')
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            user_id BIGINT NOT NULL UNIQUE
        )
        ''')
        
        countries = get_all_countries(cursor)
        for country in countries:
            table_name = f"posts_{country['country_name']}"
            create_country_posts_table(cursor, table_name)
        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Database initialization completed successfully")
    except Exception as e:
        logging.error(f"Database initialization error: {e}")
        logging.error(traceback.format_exc())
        raise

def create_country_posts_table(cursor, table_name):
    try:
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            post_date DATE NOT NULL,
            channel_id BIGINT NOT NULL,
            channel_name TEXT NOT NULL,
            content TEXT
        )
        ''')
        logging.info(f"{table_name} table check completed")
        return True
    except Exception as e:
        logging.error(f"Error creating table {table_name}: {e}")
        logging.error(traceback.format_exc())
        return False

def get_all_countries(cursor=None):
    try:
        close_connection = False
        if cursor is None:
            conn = connect_to_db()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            close_connection = True
        
        cursor.execute("SELECT * FROM country_configs ORDER BY country_name")
        countries = cursor.fetchall()
        
        if close_connection:
            cursor.close()
            conn.close()
        
        return countries
    except Exception as e:
        logging.error(f"Error getting countries: {e}")
        logging.error(traceback.format_exc())
        return []

def add_country_config(country_name, api_id, api_hash, phone_number):
    try:
        logging.info(f"Adding new country: {country_name}")
        conn = connect_to_db()
        cursor = conn.cursor()
        
        session_name = f"{country_name}_session"
        
        cursor.execute(
            "SELECT country_name FROM country_configs WHERE country_name = %s", 
            (country_name,)
        )
        if cursor.fetchone():
            cursor.execute(
                "UPDATE country_configs SET api_id = %s, api_hash = %s, phone_number = %s, session_name = %s WHERE country_name = %s",
                (api_id, api_hash, phone_number, session_name, country_name)
            )
            logging.info(f"Updated config for country: {country_name}")
        else:
            cursor.execute(
                "INSERT INTO country_configs (country_name, api_id, api_hash, phone_number, session_name) VALUES (%s, %s, %s, %s, %s)",
                (country_name, api_id, api_hash, phone_number, session_name)
            )
            logging.info(f"Added new country config: {country_name}")
            
            table_name = f"posts_{country_name}"
            create_country_posts_table(cursor, table_name)
        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Successfully added/updated country config: {country_name}")
        return True
    except Exception as e:
        logging.error(f"Error adding country config: {e}")
        logging.error(traceback.format_exc())
        return False

def remove_country_config(country_name):
    try:
        logging.info(f"Removing country: {country_name}")
        conn = connect_to_db()
        cursor = conn.cursor()
        
        cursor.execute("SELECT country_name FROM country_configs WHERE country_name = %s", (country_name,))
        if not cursor.fetchone():
            return False
        
        cursor.execute("DELETE FROM country_configs WHERE country_name = %s", (country_name,))
        logging.info(f"Removed country config: {country_name}")
        
        cursor.execute("DELETE FROM channels WHERE country = %s", (country_name,))
        
        table_name = f"posts_{country_name}"
        backup_table = f"{table_name}_backup_{int(datetime.now().timestamp())}"
        cursor.execute(f"ALTER TABLE IF EXISTS {table_name} RENAME TO {backup_table}")
        logging.info(f"Created backup of posts table: {backup_table}")
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logging.error(f"Error removing country config: {e}")
        logging.error(traceback.format_exc())
        return False

def add_channel_to_db(channel_id, channel_name, country):
    try:
        logging.info(f"Adding/updating channel for {country}: {channel_name} (ID: {channel_id})")
        conn = connect_to_db()
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            id SERIAL PRIMARY KEY,
            channel_id BIGINT,
            channel_name TEXT NOT NULL,
            country TEXT NOT NULL,
            UNIQUE(channel_id, country)
        )
        ''')
        conn.commit()
        
        cursor.execute(
            "SELECT channel_id FROM channels WHERE channel_id = %s AND country = %s", 
            (channel_id, country)
        )
        result = cursor.fetchone()
        if not result:
            cursor.execute(
                "INSERT INTO channels (channel_id, channel_name, country) VALUES (%s, %s, %s)",
                (channel_id, channel_name, country)
            )
            logging.info(f"New channel added for {country}: {channel_name} (ID: {channel_id})")
        else:
            cursor.execute(
                "UPDATE channels SET channel_name = %s WHERE channel_id = %s AND country = %s",
                (channel_name, channel_id, country)
            )
            logging.info(f"Channel updated for {country}: {channel_name} (ID: {channel_id})")
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error adding/updating channel: {e}")
        logging.error(traceback.format_exc())

def add_post_to_db(post_date, channel_id, channel_name, content, country):
    try:
        logging.info(f"Adding post - Country: {country}, Channel: {channel_name}, Date: {post_date}, Content: {content[:50]}...")
        conn = connect_to_db()
        cursor = conn.cursor()
        
        table_name = f"posts_{country.lower()}"
        create_country_posts_table(cursor, table_name)
        conn.commit()
        
        query = f"""
        INSERT INTO {table_name} (post_date, channel_id, channel_name, content) 
        VALUES (%s, %s, %s, %s)
        """
        
        cursor.execute(query, (post_date, channel_id, channel_name, content))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Post added from {country} channel {channel_name} dated {post_date}")
    except Exception as e:
        logging.error(f"Error adding post to {country} table: {e}")
        logging.error(traceback.format_exc())

def get_posts_by_period(start_date, end_date, country):
    try:
        logging.info(f"Retrieving {country} posts from {start_date} to {end_date}")
        conn = connect_to_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        table_name = f"posts_{country.lower()}"
        cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            post_date DATE NOT NULL,
            channel_id BIGINT NOT NULL,
            channel_name TEXT NOT NULL,
            content TEXT
        )
        ''')
        conn.commit()
        
        query = f"""
        SELECT post_date, channel_name, content
        FROM {table_name}
        WHERE post_date BETWEEN %s AND %s
        ORDER BY post_date DESC
        """
        
        cursor.execute(query, (start_date, end_date))
        posts = cursor.fetchall()
        logging.info(f"Retrieved {len(posts)} {country} posts")
        cursor.close()
        conn.close()
        return posts
    except Exception as e:
        logging.error(f"Error retrieving {country} posts: {e}")
        logging.error(traceback.format_exc())
        return []

def export_to_excel(posts, filename):
    try:
        logging.info(f"Exporting to Excel: {len(posts)} records")
        df = pd.DataFrame(posts)
        df = df.rename(columns={
            'post_date': 'Дата публикации',
            'channel_name': 'Источник',
            'content': 'Текст'
        })
        df.to_excel(filename, index=False)
        logging.info(f"Exported to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Error exporting to Excel: {e}")
        logging.error(traceback.format_exc())
        raise

def export_to_json(posts, filename):
    try:
        logging.info(f"Exporting to JSON: {len(posts)} records")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(posts, f, ensure_ascii=False, indent=4, default=str)
        logging.info(f"Exported to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Error exporting to JSON: {e}")
        logging.error(traceback.format_exc())
        raise

def export_all_to_json():
    try:
        logging.info("Exporting all data to JSON")
        conn = connect_to_db()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        countries = get_all_countries(cursor)
        all_data = []
        
        for country in countries:
            country_name = country['country_name']
            table_name = f"posts_{country_name.lower()}"
            cursor.execute(f'''
            SELECT post_date, channel_name, content
            FROM {table_name}
            ORDER BY post_date DESC
            ''')
            posts = cursor.fetchall()
            for post in posts:
                post['country'] = get_country_display_name(country_name)
            all_data.extend(posts)
        
        cursor.close()
        conn.close()
        
        if not all_data:
            logging.info("No data to export")
            return None
        
        filename = f"all_posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=4, default=str)
        logging.info(f"Exported all data to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Error exporting all data to JSON: {e}")
        logging.error(traceback.format_exc())
        return None

def is_user(user_id):
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return bool(result)
    except Exception as e:
        logging.error(f"Error checking user: {e}")
        return False

def add_user(user_id):
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("INSERT INTO users (user_id) VALUES (%s) ON CONFLICT (user_id) DO NOTHING", (user_id,))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"User {user_id} added successfully")
        user_logger.info(f"Added user {user_id}", extra={'user_id': user_id})
        return True
    except Exception as e:
        logging.error(f"Error adding user: {e}")
        return False

def remove_user(user_id):
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM users WHERE user_id = %s", (user_id,))
        affected_rows = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        if affected_rows > 0:
            logging.info(f"User {user_id} removed successfully")
            user_logger.info(f"Removed user {user_id}", extra={'user_id': user_id})
            return True
        return False
    except Exception as e:
        logging.error(f"Error removing user: {e}")
        return False

def get_users():
    try:
        conn = connect_to_db()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM users")
        users = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return users
    except Exception as e:
        logging.error(f"Error retrieving users: {e}")
        return []

async def create_new_client(country_config):
    try:
        country_name = country_config['country_name']
        client = TelegramClient(
            country_config['session_name'],
            country_config['api_id'],
            country_config['api_hash'],
            connection_retries=5
        )
        
        async def create_listener_for_country(specific_country):
            @client.on(events.NewMessage)
            async def new_post_listener(event):
                if isinstance(event.peer_id, PeerChannel):
                    try:
                        channel = await client.get_entity(event.peer_id)
                        channel_id = event.peer_id.channel_id
                        channel_name = channel.title
                        
                        add_channel_to_db(channel_id, channel_name, specific_country)
                        
                        if event.is_group or event.is_channel:
                            post_date_utc = event.message.date
                            moscow_tz = pytz.timezone('Europe/Moscow')
                            post_date_moscow = post_date_utc.astimezone(moscow_tz)
                            post_date = post_date_moscow.strftime('%Y-%m-%d')
                            post_content = event.message.text or "Медиа-сообщение (без текста)"
                            
                            add_post_to_db(post_date, channel_id, channel_name, post_content, specific_country)
                    except Exception as e:
                        logging.error(f"Error processing {specific_country} post: {e}")
                        logging.error(traceback.format_exc())
            return new_post_listener
        
        await create_listener_for_country(country_name)
        
        await client.connect()
        if not await client.is_user_authorized():
            try:
                await client.send_code_request(country_config['phone_number'])
                logging.info(f"Auth code requested for {country_name}")
                return {'client': client, 'needs_code': True, 'country_name': country_name}
            except FloodWaitError as e:
                logging.error(f"Flood wait error for {country_name}: {e.seconds} seconds")
                return {'client': None, 'needs_code': False, 'error': f"Flood wait: {e.seconds} seconds", 'country_name': country_name}
            except Exception as e:
                logging.error(f"Auth error for {country_name}: {e}")
                return {'client': None, 'needs_code': False, 'error': str(e), 'country_name': country_name}
        
        logging.info(f"{country_name} client started successfully")
        active_clients[country_name] = client
        return {'client': client, 'needs_code': False, 'country_name': country_name}
    except Exception as e:
        logging.error(f"Error creating client for {country_name}: {e}")
        logging.error(traceback.format_exc())
        return {'client': None, 'needs_code': False, 'error': str(e), 'country_name': country_name}

async def check_clients():
    while True:
        try:
            logging.info("Checking status of active clients...")
            countries = get_all_countries()
            for country in countries:
                country_name = country['country_name']
                client = active_clients.get(country_name)
                
                if client is None or not client.is_connected():
                    logging.warning(f"Client for {country_name} is disconnected or not initialized. Attempting to restart...")
                    result = await create_new_client(country)
                    if result.get('error'):
                        logging.error(f"Failed to restart client for {country_name}: {result['error']}")
                    elif result['needs_code']:
                        logging.warning(f"Client for {country_name} requires manual authorization")
                    else:
                        logging.info(f"Client for {country_name} restarted successfully")
                
                else:
                    logging.debug(f"Client for {country_name} is active")
            
            await asyncio.sleep(300)
        except Exception as e:
            logging.error(f"Error in check_clients: {e}")
            logging.error(traceback.format_exc())
            await asyncio.sleep(60)

def get_country_emoji(country_name):
    country_emojis = {
        "afghanistan": "🇦🇫", "albania": "🇦🇱", "algeria": "🇩🇿", "andorra": "🇦🇩", "angola": "🇦🇴",
        "antigua_and_barbuda": "🇦🇬", "argentina": "🇦🇷", "armenia": "🇦🇲", "australia": "🇦🇺", "austria": "🇦🇹",
        "azerbaijan": "🇦🇿", "bahamas": "🇧🇸", "bahrain": "🇧🇭", "bangladesh": "🇧🇩", "barbados": "🇧🇧",
        "belarus": "🇧🇾", "belgium": "🇧🇪", "belize": "🇧🇿", "benin": "🇧🇯", "bhutan": "🇧🇹",
        "bolivia": "🇧🇴", "bosnia_and_herzegovina": "🇧🇦", "botswana": "🇧🇼", "brazil": "🇧🇷", "brunei": "🇧🇳",
        "bulgaria": "🇧🇬", "burkina_faso": "🇧🇫", "burundi": "🇧🇮", "cabo_verde": "🇨🇻", "cambodia": "🇰🇭",
        "cameroon": "🇨🇲", "canada": "🇨🇦", "central_african_republic": "🇨🇫", "chad": "🇹🇩", "chile": "🇨🇱",
        "china": "🇨🇳", "colombia": "🇨🇴", "comoros": "🇰🇲", "congo": "🇨🇬", "costa_rica": "🇨🇷",
        "croatia": "🇭🇷", "cuba": "🇨🇺", "cyprus": "🇨🇾", "czech_republic": "🇨🇿", "denmark": "🇩🇰",
        "djibouti": "🇩🇯", "dominica": "🇩🇲", "dominican_republic": "🇩🇴", "ecuador": "🇪🇨", "egypt": "🇪🇬",
        "el_salvador": "🇸🇻", "equatorial_guinea": "🇬🇶", "eritrea": "🇪🇷", "estonia": "🇪🇪", "eswatini": "🇸🇿",
        "ethiopia": "🇪🇹", "fiji": "🇫🇯", "finland": "🇫🇮", "france": "🇫🇷", "gabon": "🇬🇦",
        "gambia": "🇬🇲", "georgia": "🇬🇪", "germany": "🇩🇪", "ghana": "🇬🇭", "greece": "🇬🇷",
        "grenada": "🇬🇩", "guatemala": "🇬🇹", "guinea": "🇬🇳", "guinea_bissau": "🇬🇼", "guyana": "🇬🇾",
        "haiti": "🇭🇹", "honduras": "🇭🇳", "hungary": "🇭🇺", "iceland": "🇮🇸", "india": "🇮🇳",
        "indonesia": "🇮🇩", "iran": "🇮🇷", "iraq": "🇮🇶", "ireland": "🇮🇪", "israel": "🇮🇱",
        "italy": "🇮🇹", "jamaica": "🇯🇲", "japan": "🇯🇵", "jordan": "🇯🇴", "kazakhstan": "🇰🇿",
        "kenya": "🇰🇪", "kiribati": "🇰🇮", "north_korea": "🇰🇵", "south_korea": "🇰🇷", "kuwait": "🇰🇼",
        "kyrgyzstan": "🇰🇬", "laos": "🇱🇦", "latvia": "🇱🇻", "lebanon": "🇱🇧", "lesotho": "🇱🇸",
        "liberia": "🇱🇷", "libya": "🇱🇾", "liechtenstein": "🇱🇮", "lithuania": "🇱🇹", "luxembourg": "🇱🇺",
        "madagascar": "🇲🇬", "malawi": "🇲🇼", "malaysia": "🇲🇾", "maldives": "🇲🇻", "mali": "🇲🇱",
        "malta": "🇲🇹", "marshall_islands": "🇲🇭", "mauritania": "🇲🇷", "mauritius": "🇲🇺", "mexico": "🇲🇽",
        "micronesia": "🇫🇲", "moldova": "🇲🇩", "monaco": "🇲🇨", "mongolia": "🇲🇳", "montenegro": "🇲🇪",
        "morocco": "🇲🇦", "mozambique": "🇲🇿", "myanmar": "🇲🇲", "namibia": "🇳🇦", "nauru": "🇳🇷",
        "nepal": "🇳🇵", "netherlands": "🇳🇱", "new_zealand": "🇳🇿", "nicaragua": "🇳🇮", "niger": "🇳🇪",
        "nigeria": "🇳🇬", "north_macedonia": "🇲🇰", "norway": "🇳🇴", "oman": "🇴🇲", "pakistan": "🇵🇰",
        "palau": "🇵🇼", "panama": "🇵🇦", "papua_new_guinea": "🇵🇬", "paraguay": "🇵🇾", "peru": "🇵🇪",
        "philippines": "🇵🇭", "poland": "🇵🇱", "portugal": "🇵🇹", "qatar": "🇶🇦", "romania": "🇷🇴",
        "russia": "🇷🇺", "rwanda": "🇷🇼", "saint_kitts_and_nevis": "🇰🇳", "saint_lucia": "🇱🇨",
        "saint_vincent_and_the_grenadines": "🇻🇚", "samoa": "🇼🇸", "san_marino": "🇸🇲", "sao_tome_and_principe": "🇸🇹",
        "saudi_arabia": "🇸🇦", "senegal": "🇸🇳", "serbia": "🇷🇸", "seychelles": "🇸🇨", "sierra_leone": "🇸🇱",
        "singapore": "🇸🇬", "slovakia": "🇸🇰", "slovenia": "🇸🇮", "solomon_islands": "🇸🇧", "somalia": "🇸🇴",
        "south_africa": "🇿🇦", "south_sudan": "🇸🇸", "spain": "🇪🇸", "sri_lanka": "🇱🇰", "sudan": "🇸🇩",
        "suriname": "🇸🇷", "sweden": "🇸🇪", "switzerland": "🇨🇭", "syria": "🇸🇾", "taiwan": "🇹🇼",
        "tajikistan": "🇹🇯", "tanzania": "🇹🇿", "thailand": "🇹🇭", "timor_leste": "🇹🇱", "togo": "🇹🇬",
        "tonga": "🇹🇴", "trinidad_and_tobago": "🇹🇹", "tunisia": "🇹🇳", "turkey": "🇹🇷", "turkmenistan": "🇹🇲",
        "tuvalu": "🇹🇻", "uganda": "🇺🇬", "ukraine": "🇺🇦", "united_arab_emirates": "🇦🇪", "uk": "🇬🇧",
        "usa": "🇺🇸", "uruguay": "🇺🇾", "uzbekistan": "🇺🇿", "vanuatu": "🇻🇺", "venezuela": "🇻🇪",
        "vietnam": "🇻🇳", "yemen": "🇾🇪", "zambia": "🇿🇲", "zimbabwe": "🇿🇼"
    }
    return country_emojis.get(country_name.lower(), "🌍")

def get_country_display_name(country_name):
    country_mappings = {
        "afghanistan": "Афганистан", "albania": "Албания", "algeria": "Алжир", "andorra": "Андорра", "angola": "Ангола",
        "antigua_and_barbuda": "Антигуа и Барбуда", "argentina": "Аргентина", "armenia": "Армения", "australia": "Австралия",
        "austria": "Австрия", "azerbaijan": "Азербайджан", "bahamas": "Багамы", "bahrain": "Бахрейн", "bangladesh": "Бангладеш",
        "barbados": "Барбадос", "belarus": "Беларусь", "belgium": "Бельгия", "belize": "Белиз", "benin": "Бенин",
        "bhutan": "Бутан", "bolivia": "Боливия", "bosnia_and_herzegovina": "Босния и Герцеговина", "botswana": "Ботсвана",
        "brazil": "Бразилия", "brunei": "Бруней", "bulgaria": "Болгария", "burkina_faso": "Буркина-Фасо", "burundi": "Бурунди",
        "cabo_verde": "Кабо-Верде", "cambodia": "Камбоджа", "cameroon": "Камерун", "canada": "Канада",
        "central_african_republic": "Центральноафриканская Республика", "chad": "Чад", "chile": "Чили", "china": "Китай",
        "colombia": "Колумбия", "comoros": "Коморы", "congo": "Конго", "costa_rica": "Коста-Рика", "croatia": "Хорватия",
        "cuba": "Куба", "cyprus": "Кипр", "czech_republic": "Чехия", "denmark": "Дания", "djibouti": "Джибути",
        "dominica": "Доминика", "dominican_republic": "Доминиканская Республика", "ecuador": "Эквадор", "egypt": "Египет",
        "el_salvador": "Сальвадор", "equatorial_guinea": "Экваториальная Гвинея", "eritrea": "Эритрея", "estonia": "Эстония",
        "eswatini": "Эсватини", "ethiopia": "Эфиопия", "fiji": "Фиджи", "finland": "Финляндия", "france": "Франция",
        "gabon": "Габон", "gambia": "Гамбия", "georgia": "Грузия", "germany": "Германия", "ghana": "Гана",
        "greece": "Греция", "grenada": "Гренада", "guatemala": "Гватемала", "guinea": "Гвинея", "guinea_bissau": "Гвинея-Бисау",
        "guyana": "Гайана", "haiti": "Гаити", "honduras": "Гондурас", "hungary": "Венгрия", "iceland": "Исландия",
        "india": "Индия", "indonesia": "Индонезия", "iran": "Иран", "iraq": "Ирак", "ireland": "Ирландия",
        "israel": "Израиль", "italy": "Италия", "jamaica": "Ямайка", "japan": "Япония", "jordan": "Иордания",
        "kazakhstan": "Казахстан", "kenya": "Кения", "kiribati": "Кирибати", "north_korea": "Северная Корея",
        "south_korea": "Южная Корея", "kuwait": "Кувейт", "kyrgyzstan": "Кыргызстан", "laos": "Лаос", "latvia": "Латвия",
        "lebanon": "Ливан", "lesotho": "Лесото", "liberia": "Либерия", "libya": "Ливия", "liechtenstein": "Лихтенштейн",
        "lithuania": "Литва", "luxembourg": "Люксембург", "madagascar": "Мадагаскар", "malawi": "Малави",
        "malaysia": "Малайзия", "maldives": "Мальдивы", "mali": "Мали", "malta": "Мальта", "marshall_islands": "Маршалловы Острова",
        "mauritania": "Мавритания", "mauritius": "Маврикий", "mexico": "Мексика", "micronesia": "Микронезия",
        "moldova": "Молдавия", "monaco": "Монако", "mongolia": "Монголия", "montenegro": "Черногория", "morocco": "Марокко",
        "mozambique": "Мозамбик", "myanmar": "Мьянма", "namibia": "Намибия", "nauru": "Науру", "nepal": "Непал",
        "netherlands": "Нидерланды", "new_zealand": "Новая Зеландия", "nicaragua": "Никарагуа", "niger": "Нигер",
        "nigeria": "Нигерия", "north_macedonia": "Северная Македония", "norway": "Норвегия", "oman": "Оман",
        "pakistan": "Пакистан", "palau": "Палау", "panama": "Панама", "papua_new_guinea": "Папуа — Новая Гвинея",
        "paraguay": "Парагвай", "peru": "Перу", "philippines": "Филиппины", "poland": "Польша", "portugal": "Португалия",
        "qatar": "Катар", "romania": "Румыния", "russia": "Россия", "rwanda": "Руанда", "saint_kitts_and_nevis": "Сент-Китс и Невис",
        "saint_lucia": "Сент-Люсия", "saint_vincent_and_the_grenadines": "Сент-Винсент и Гренадины", "samoa": "Самоа",
        "san_marino": "Сан-Марино", "sao_tome_and_principe": "Сан-Томе и Принсипи", "saudi_arabia": "Саудовская Аравия",
        "senegal": "Сенегал", "serbia": "Сербия", "seychelles": "Сейшелы", "sierra_leone": "Сьерра-Леоне",
        "singapore": "Сингапур", "slovakia": "Словакия", "slovenia": "Словения", "solomon_islands": "Соломоновы Острова",
        "somalia": "Сомали", "south_africa": "Южная Африка", "south_sudan": "Южный Судан", "spain": "Испания",
        "sri_lanka": "Шри-Ланка", "sudan": "Судан", "suriname": "Суринам", "sweden": "Швеция", "switzerland": "Швейцария",
        "syria": "Сирия", "taiwan": "Тайвань", "tajikistan": "Таджикистан", "tanzania": "Танзания", "thailand": "Таиланд",
        "timor_leste": "Восточный Тимор", "togo": "Того", "tonga": "Тонга", "trinidad_and_tobago": "Тринидад и Тобаго",
        "tunisia": "Тунис", "turkey": "Турция", "turkmenistan": "Туркменистан", "tuvalu": "Тувалу", "uganda": "Уганда",
        "ukraine": "Украина", "united_arab_emirates": "ОАЭ", "uk": "Великобритания", "usa": "США", "uruguay": "Уругвай",
        "uzbekistan": "Узбекистан", "vanuatu": "Вануату", "venezuela": "Венесуэла", "vietnam": "Вьетнам", "yemen": "Йемен",
        "zambia": "Замбия", "zimbabwe": "Зимбабве"
    }
    return country_mappings.get(country_name.lower(), country_name.capitalize())

# Function to create reply keyboard with only "List Countries" button
def create_start_keyboard():
    buttons = [[KeyboardButton(text="🌎 Список стран")]]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

# Function to create inline keyboard with paginated countries (10 per page)
def create_paginated_countries_inline(countries, page=1):
    countries_per_page = 10
    total_pages = (len(countries) + countries_per_page - 1) // countries_per_page
    page = max(1, min(page, total_pages))  # Ensure page is within bounds
    
    start_idx = (page - 1) * countries_per_page
    end_idx = min(start_idx + countries_per_page, len(countries))
    
    buttons = []
    for country in countries[start_idx:end_idx]:
        country_name = country['country_name']
        display_name = f"{get_country_emoji(country_name)} {get_country_display_name(country_name)}"
        buttons.append([InlineKeyboardButton(text=display_name, callback_data=f"country_{country_name}")])
    
    # Pagination buttons
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"page_{page-1}"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton(text="Вперед ➡️", callback_data=f"page_{page+1}"))
    
    if nav_buttons:
        buttons.append(nav_buttons)
    
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# Function to display countries list as inline buttons
async def show_countries_list_inline(message: types.Message, page=1):
    countries = get_all_countries()
    if not countries:
        await message.answer("⚠️ Нет настроенных стран", reply_markup=create_start_keyboard())
        return
    
    keyboard = create_paginated_countries_inline(countries, page)
    await message.answer(f"🌍 <b>Выберите страну (страница {page}):</b>", 
                        reply_markup=keyboard, 
                        parse_mode="HTML")

# Global command handler
@dp.message(Command(commands=["start", "log", "log_users", "settings", "dump", "admin"]))
async def handle_global_commands(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    command = message.text
    user_logger.info(f"Command executed: {command}", extra={'user_id': user_id})
    
    if not is_user(user_id):
        user_logger.info(f"Access denied: User {user_id} not authorized", extra={'user_id': user_id})
        await message.answer("Нет доступа", reply_markup=ReplyKeyboardRemove())
        return
    
    if command == "/start":
        user_logger.info(f"Started bot", extra={'user_id': user_id})
        countries = get_all_countries()
        if not countries:
            user_logger.info(f"No countries available", extra={'user_id': user_id})
            await message.answer("⚠️ Нет стран", reply_markup=create_start_keyboard())
            return
        
        await state.clear()
        await show_countries_list_inline(message)
        await state.set_state(ExportStates.waiting_for_country)
        return
    
    if command == "/settings":
        user_logger.info(f"Accessed settings", extra={'user_id': user_id})
        await state.clear()
        await message.answer("🔐 Введите пароль для управления странами:", reply_markup=ReplyKeyboardRemove())
        await state.set_state(ManageCountryStates.waiting_for_password)
        return
    
    if command == "/log":
        user_logger.info(f"Requested log file", extra={'user_id': user_id})
        await state.clear()
        await message.answer("🔐 Введите пароль для доступа к логам:", reply_markup=ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_log_password)
        return
    
    if command == "/log_users":
        user_logger.info(f"Requested user actions log", extra={'user_id': user_id})
        await state.clear()
        await message.answer("🔐 Введите пароль для доступа к логам пользователей:", reply_markup=ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_user_log_password)
        return
    
    if command == "/dump":
        user_logger.info(f"Requested database dump", extra={'user_id': user_id})
        await state.clear()
        await message.answer("🔐 Введите пароль для создания дампа:", reply_markup=ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_dump_password)
        return
    
    if command == "/admin":
        user_logger.info(f"Accessed admin panel", extra={'user_id': user_id})
        await state.clear()
        await message.answer("🔐 Введите пароль для управления пользователями:", reply_markup=ReplyKeyboardRemove())
        await state.set_state(AdminStates.waiting_for_admin_password)
        return

@dp.message(ExportStates.waiting_for_country)
async def process_country_selection(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    selected_text = message.text.strip()
    user_logger.info(f"Received input: {selected_text}", extra={'user_id': user_id})
    
    if selected_text == "🌎 Список стран":
        countries = get_all_countries()
        if not countries:
            user_logger.info(f"No countries available", extra={'user_id': user_id})
            await message.answer("⚠️ Нет стран", reply_markup=create_start_keyboard())
            return
        
        await state.clear()
        await show_countries_list_inline(message)
        await state.set_state(ExportStates.waiting_for_country)
        return
    
    await message.answer("⚠️ Пожалуйста, выберите страну из списка, нажав на inline-кнопку.", reply_markup=create_start_keyboard())

# Handle inline country selection
@dp.callback_query(lambda c: c.data.startswith("country_") or c.data.startswith("page_"))
async def process_country_list_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    callback_data = callback.data
    user_logger.info(f"Callback received: {callback_data}", extra={'user_id': user_id})
    
    if callback_data.startswith("page_"):
        page = int(callback_data[len("page_"):])
        await callback.message.delete()
        await show_countries_list_inline(callback.message, page)
        await callback.answer()
        return
    
    if callback_data.startswith("country_"):
        selected_country = callback_data[len("country_"):]
        await state.update_data(country=selected_country)
        user_logger.info(f"Selected country: {selected_country}", extra={'user_id': user_id})
        
        buttons = [
            [
                InlineKeyboardButton(text="Сегодня", callback_data="period_today"),
                InlineKeyboardButton(text="Вчера", callback_data="period_yesterday")
            ],
            [
                InlineKeyboardButton(text="3 дня", callback_data="period_3days"),
                InlineKeyboardButton(text="Текущий месяц", callback_data="period_current_month")
            ],
            [
                InlineKeyboardButton(text="Конкретный месяц", callback_data="period_custom_month"),
                InlineKeyboardButton(text="Все время", callback_data="period_all_time")
            ],
            [
                InlineKeyboardButton(text="◶ Назад", callback_data="back_to_countries")
            ]
        ]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.delete()
        await callback.message.answer(
            "Выберите период:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.set_state(ExportStates.waiting_for_period)
        await callback.answer()

# Handle period selection
@dp.callback_query(ExportStates.waiting_for_period)
async def process_period_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    callback_data = callback.data
    user_logger.info(f"Selected period: {callback_data}", extra={'user_id': user_id})
    
    try:
        if callback_data == "back_to_countries":
            await callback.message.delete()
            await show_countries_list_inline(callback.message)
            await callback.message.answer("Нажмите 'Список стран' для выбора страны:", reply_markup=create_start_keyboard())
            await state.set_state(ExportStates.waiting_for_country)
            await callback.answer()
            return
        
        user_data = await state.get_data()
        country = user_data.get('country')
        today = datetime.now().date()
        current_year = today.year
        
        start_date = None
        end_date = None
        period_name = ""
        
        if callback_data == "period_today":
            start_date = today
            end_date = today
            period_name = "сегодня"
        elif callback_data == "period_yesterday":
            start_date = today - timedelta(days=1)
            end_date = today - timedelta(days=1)
            period_name = "вчера"
        elif callback_data == "period_3days":
            start_date = today - timedelta(days=2)
            end_date = today
            period_name = "3 дня"
        elif callback_data == "period_current_month":
            start_date = today.replace(day=1)
            end_date = today
            period_name = "текущий месяц"
        elif callback_data == "period_custom_month":
            # Create inline keyboard with 12 months
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            buttons = []
            for i, month_name in enumerate(month_names, 1):
                buttons.append(InlineKeyboardButton(text=month_name, callback_data=f"month_{i}"))
            
            # Organize buttons into 3 rows of 4 buttons each
            keyboard_buttons = [
                buttons[0:4],  # Январь - Апрель
                buttons[4:8],  # Май - Август
                buttons[8:12], # Сентябрь - Декабрь
                [InlineKeyboardButton(text="◶ Назад", callback_data="back_to_period")]
            ]
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)
            
            await callback.message.delete()
            await callback.message.answer(
                f"📅 Выберите месяц для {current_year} года:",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            await state.set_state(ExportStates.waiting_for_custom_month)
            await callback.answer()
            return
        elif callback_data == "period_all_time":
            start_date = datetime(2000, 1, 1).date()
            end_date = today
            period_name = "все время"
        
        await state.update_data(start_date=start_date, end_date=end_date, period_name=period_name)
        user_logger.info(f"Period set: {period_name} ({start_date} - {end_date}) for {country}", extra={'user_id': user_id})
        
        buttons = [
            [
                InlineKeyboardButton(text="Excel", callback_data="format_excel"),
                InlineKeyboardButton(text="JSON", callback_data="format_json")
            ],
            [
                InlineKeyboardButton(text="◶ Назад", callback_data="back_to_period")
            ]
        ]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.delete()
        await callback.message.answer("Выберите формат:", reply_markup=keyboard)
        await state.set_state(ExportStates.waiting_for_format)
        await callback.answer()
    except Exception as e:
        logging.error(f"Error in process_period_callback for user {user_id}: {e}")
        logging.error(traceback.format_exc())
        await callback.message.answer("⚠️ Произошла ошибка. Попробуйте снова.", reply_markup=create_start_keyboard())
        await callback.answer()

@dp.callback_query(ExportStates.waiting_for_custom_month)
async def process_custom_month_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    callback_data = callback.data
    user_logger.info(f"Selected custom month: {callback_data}", extra={'user_id': user_id})
    
    try:
        if callback_data == "back_to_period":
            buttons = [
                [
                    InlineKeyboardButton(text="Сегодня", callback_data="period_today"),
                    InlineKeyboardButton(text="Вчера", callback_data="period_yesterday")
                ],
                [
                    InlineKeyboardButton(text="3 дня", callback_data="period_3days"),
                    InlineKeyboardButton(text="Текущий месяц", callback_data="period_current_month")
                ],
                [
                    InlineKeyboardButton(text="Конкретный месяц", callback_data="period_custom_month"),
                    InlineKeyboardButton(text="Все время", callback_data="period_all_time")
                ],
                [
                    InlineKeyboardButton(text="◶ Назад", callback_data="back_to_countries")
                ]
            ]
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
            
            await callback.message.delete()
            await callback.message.answer("Выберите период:", reply_markup=keyboard)
            await state.set_state(ExportStates.waiting_for_period)
            await callback.answer()
            return
        
        if callback_data.startswith("month_"):
            month = int(callback_data[len("month_"):])
            current_year = datetime.now().year
            
            start_date = datetime(current_year, month, 1).date()
            if month == 12:
                end_date = datetime(current_year, month, 31).date()
            else:
                end_date = (datetime(current_year, month + 1, 1) - timedelta(days=1)).date()
            
            user_data = await state.get_data()
            country = user_data.get('country')
            month_names = [
                "Январь", "Февраль", "Март", "Апрель", "Май", "Июнь",
                "Июль", "Август", "Сентябрь", "Октябрь", "Ноябрь", "Декабрь"
            ]
            period_name = f"{month_names[month-1]} {current_year}"
            
            await state.update_data(start_date=start_date, end_date=end_date, period_name=period_name)
            user_logger.info(f"Set custom period: {period_name} ({start_date} - {end_date}) for {country}", extra={'user_id': user_id})
            
            buttons = [
                [
                    InlineKeyboardButton(text="Excel", callback_data="format_excel"),
                    InlineKeyboardButton(text="JSON", callback_data="format_json")
                ],
                [
                    InlineKeyboardButton(text="◶ Назад", callback_data="back_to_period")
                ]
            ]
            
            keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
            
            await callback.message.delete()
            await callback.message.answer("Выберите формат:", reply_markup=keyboard)
            await state.set_state(ExportStates.waiting_for_format)
            await callback.answer()
    except Exception as e:
        logging.error(f"Error in process_custom_month_callback for user {user_id}: {e}")
        logging.error(traceback.format_exc())
        await callback.message.answer("⚠️ Произошла ошибка. Попробуйте снова.", reply_markup=create_start_keyboard())
        await callback.answer()

# Handle /log password
@dp.message(AdminStates.waiting_for_log_password)
async def process_log_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_logger.info(f"Entered password for /log", extra={'user_id': user_id})
    
    if message.text != PASSWORD:
        user_logger.info(f"Incorrect password for /log", extra={'user_id': user_id})
        await message.answer("⚠️ Неверный пароль", reply_markup=create_start_keyboard())
        await state.clear()
        await handle_global_commands(message, state)
        return
    
    trim_log_file(LOG_FILE)
    
    if not os.path.exists(LOG_FILE):
        user_logger.info(f"Log file not found", extra={'user_id': user_id})
        await message.answer("⚠️ Лог-файл не найден", reply_markup=create_start_keyboard())
        await state.clear()
        await handle_global_commands(message, state)
        return
    
    await message.answer_document(
        document=FSInputFile(LOG_FILE),
        caption=f"Лог-файл ({MAX_LOG_LINES} строк максимум)"
    )
    user_logger.info(f"Sent bot log file", extra={'user_id': user_id})
    await state.clear()
    await handle_global_commands(message, state)

# Handle /log_users password
@dp.message(AdminStates.waiting_for_user_log_password)
async def process_user_log_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_logger.info(f"Entered password for /log_users", extra={'user_id': user_id})
    
    if message.text != PASSWORD:
        user_logger.info(f"Incorrect password for /log_users", extra={'user_id': user_id})
        await message.answer("⚠️ Неверный пароль", reply_markup=create_start_keyboard())
        await state.clear()
        await handle_global_commands(message, state)
        return
    
    trim_log_file(USER_LOG_FILE)
    
    if not os.path.exists(USER_LOG_FILE):
        user_logger.info(f"User log file not found", extra={'user_id': user_id})
        await message.answer("⚠️ Лог-файл пользователей не найден", reply_markup=create_start_keyboard())
        await state.clear()
        await handle_global_commands(message, state)
        return
    
    await message.answer_document(
        document=FSInputFile(USER_LOG_FILE),
        caption=f"Лог действий пользователей ({MAX_LOG_LINES} строк максимум)"
    )
    user_logger.info(f"Sent user actions log file", extra={'user_id': user_id})
    await state.clear()
    await handle_global_commands(message, state)

# Handle /dump password
@dp.message(AdminStates.waiting_for_dump_password)
async def process_dump_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_logger.info(f"Entered password for /dump", extra={'user_id': user_id})
    
    if message.text != PASSWORD:
        user_logger.info(f"Incorrect password for /dump", extra={'user_id': user_id})
        await message.answer("⚠️ Неверный пароль", reply_markup=create_start_keyboard())
        await state.clear()
        await handle_global_commands(message, state)
        return
    
    dump_file = export_all_to_json()
    
    if dump_file:
        await message.answer_document(
            document=FSInputFile(dump_file),
            caption="📦 Дамп всех данных (JSON)"
        )
        os.remove(dump_file)
        user_logger.info(f"Sent database dump", extra={'user_id': user_id})
    else:
        user_logger.info(f"Failed to create dump", extra={'user_id': user_id})
        await message.answer("⚠️ Ошибка создания дампа", reply_markup=create_start_keyboard())
    
    await state.clear()
    await handle_global_commands(message, state)

# Handle /admin password
@dp.message(AdminStates.waiting_for_admin_password)
async def process_admin_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_logger.info(f"Entered password for /admin", extra={'user_id': user_id})
    
    if message.text != PASSWORD:
        user_logger.info(f"Incorrect password for /admin", extra={'user_id': user_id})
        await message.answer("⚠️ Неверный пароль", reply_markup=create_start_keyboard())
        await state.clear()
        await handle_global_commands(message, state)
        return
    
    users = get_users()
    user_list = "\n".join([f"👤 {uid}" for uid in users]) if users else "Нет пользователей"
    
    buttons = [
        [InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="add_user")],
        [InlineKeyboardButton(text="❌ Удалить пользователя", callback_data="remove_user")]
    ]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await message.answer(
        f"📋 <b>Список пользователей:</b>\n{user_list}\n\nВыберите действие:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    user_logger.info(f"Displayed user management menu", extra={'user_id': user_id})

# Handle custom month input
@dp.message(ExportStates.waiting_for_custom_month)
async def process_custom_month(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_input = message.text.strip()
    user_logger.info(f"Entered custom month: {user_input}", extra={'user_id': user_id})
    
    try:
        month, year = map(int, user_input.split("."))
        if not (1 <= month <= 12 and 2000 <= year <= datetime.now().year):
            raise ValueError("Invalid date range")
        
        start_date = datetime(year, month, 1).date()
        if month == 12:
            end_date = datetime(year, month, 31).date()
        else:
            end_date = (datetime(year, month + 1, 1) - timedelta(days=1)).date()
        
        user_data = await state.get_data()
        country = user_data.get('country')
        period_name = f"{month:02d}.{year}"
        
        await state.update_data(start_date=start_date, end_date=end_date, period_name=period_name)
        user_logger.info(f"Set custom period: {period_name} ({start_date} - {end_date}) for {country}", extra={'user_id': user_id})
        
        buttons = [
            [
                InlineKeyboardButton(text="Excel", callback_data="format_excel"),
                InlineKeyboardButton(text="JSON", callback_data="format_json")
            ],
            [
                InlineKeyboardButton(text="◶ Назад", callback_data="back_to_period")
            ]
        ]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await message.answer("Выберите формат:", reply_markup=keyboard)
        await state.set_state(ExportStates.waiting_for_format)
    except ValueError:
        user_logger.info(f"Invalid custom month format: {user_input}", extra={'user_id': user_id})
        await message.answer("⚠️ Формат: ММ.ГГГГ (например, 04.2023)", reply_markup=create_start_keyboard())
        return

# Handle format selection
@dp.callback_query(ExportStates.waiting_for_format)
async def process_format_callback(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    callback_data = callback.data
    user_logger.info(f"Selected format: {callback_data}", extra={'user_id': user_id})
    
    if callback_data == "back_to_period":
        user_data = await state.get_data()
        buttons = [
            [
                InlineKeyboardButton(text="Сегодня", callback_data="period_today"),
                InlineKeyboardButton(text="Вчера", callback_data="period_yesterday")
            ],
            [
                InlineKeyboardButton(text="3 дня", callback_data="period_3days"),
                InlineKeyboardButton(text="Текущий месяц", callback_data="period_current_month")
            ],
            [
                InlineKeyboardButton(text="Конкретный месяц", callback_data="period_custom_month"),
                InlineKeyboardButton(text="Все время", callback_data="period_all_time")
            ],
            [
                InlineKeyboardButton(text="◶ Назад", callback_data="back_to_countries")
            ]
        ]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.delete()
        await callback.message.answer("Выберите период:", reply_markup=keyboard)
        await state.set_state(ExportStates.waiting_for_period)
        await callback.answer()
        return
    
    user_data = await state.get_data()
    country = user_data.get('country')
    start_date = user_data.get('start_date')
    end_date = user_data.get('end_date')
    period_name = user_data.get('period_name')
    
    posts = get_posts_by_period(start_date, end_date, country)
    
    if not posts:
        user_logger.info(f"No posts found for {country} in period {period_name}", extra={'user_id': user_id})
        await callback.message.delete()
        await callback.message.answer(f"⚠️ Нет постов за {period_name}", reply_markup=create_start_keyboard())
        await callback.answer()
        return
    
    try:
        if callback_data == "format_excel":
            filename = f"export_{country}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
            file_path = export_to_excel(posts, filename)
            
            await callback.message.delete()
            await callback.message.answer_document(
                document=FSInputFile(file_path),
                reply_markup=create_start_keyboard()  # Добавляем кнопку "Список стран"
            )
            os.remove(file_path)
            user_logger.info(f"Exported {len(posts)} posts to Excel for {country}", extra={'user_id': user_id})
            
        elif callback_data == "format_json":
            filename = f"export_{country}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.json"
            file_path = export_to_json(posts, filename)
            
            await callback.message.delete()
            await callback.message.answer_document(
                document=FSInputFile(file_path),
                reply_markup=create_start_keyboard()  # Добавляем кнопку "Список стран"
            )
            os.remove(file_path)
            user_logger.info(f"Exported {len(posts)} posts to JSON for {country}", extra={'user_id': user_id})
        
        await callback.answer()
    except Exception as e:
        logging.error(f"Export error for user {user_id}: {e}")
        await callback.message.delete()
        await callback.message.answer(f"⚠️ Ошибка экспорта: {str(e)}", reply_markup=create_start_keyboard())
        await callback.answer()

# Handle settings password
@dp.message(ManageCountryStates.waiting_for_password)
async def process_settings_password(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_logger.info(f"Entered password for settings", extra={'user_id': user_id})
    
    if message.text != PASSWORD:
        user_logger.info(f"Incorrect password for settings", extra={'user_id': user_id})
        await message.answer("⚠️ Неверный пароль", reply_markup=create_start_keyboard())
        await handle_global_commands(message, state)
        return
    
    buttons = [
        [InlineKeyboardButton(text="➕ Добавить страну", callback_data="action_add_country")],
        [InlineKeyboardButton(text="❌ Удалить страну", callback_data="action_remove_country")],
        [InlineKeyboardButton(text="<- Назад", callback_data="action_back")]
    ]
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
    
    await message.answer(
        "⚙️ <b>Управление странами</b>\n\n"
        "Выберите действие для настройки системы мониторинга:",
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await state.set_state(ManageCountryStates.waiting_for_action)

# Handle country management actions
@dp.callback_query(ManageCountryStates.waiting_for_action)
async def process_country_action(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    callback_data = callback.data
    user_logger.info(f"Selected country action: {callback_data}", extra={'user_id': user_id})
    
    if callback_data == "action_add_country":
        await callback.message.delete()
        await callback.message.answer(
            "🌍 <b>Добавление новой страны</b>\n\n"
            "Введите название страны на английском (например, 'france').\n"
            "<i>Только латинские буквы, нижний регистр, без пробелов.</i>",
            parse_mode="HTML",
            reply_markup=create_start_keyboard()
        )
        await state.set_state(ManageCountryStates.waiting_for_country_name)
        await callback.answer()
    
    elif callback_data == "action_remove_country":
        countries = get_all_countries()
        if not countries:
            user_logger.info(f"No countries to remove", extra={'user_id': user_id})
            await callback.message.delete()
            await callback.message.answer("⚠️ Нет стран для удаления", reply_markup=create_start_keyboard())
            await handle_global_commands(callback.message, state)
            await callback.answer()
            return
        
        buttons = []
        for country in countries:
            country_name = country['country_name']
            display_name = f"{get_country_emoji(country_name)} {get_country_display_name(country_name)}"
            buttons.append([InlineKeyboardButton(
                text=display_name,
                callback_data=f"remove_{country_name}"
            )])
        buttons.append([InlineKeyboardButton(text="◶ Назад", callback_data="back_to_actions")])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.delete()
        await callback.message.answer(
            "🗑️ <b>Удаление страны</b>\n\n"
            "Выберите страну для удаления.\n"
            "<i>Данные будут сохранены в резервной таблице.</i>",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.set_state(ManageCountryStates.waiting_for_country_to_remove)
        await callback.answer()
    
    elif callback_data == "action_back":
        await callback.message.delete()
        await handle_global_commands(callback.message, state)
        await callback.answer()
    
    else:
        user_logger.info(f"Invalid action: {callback_data}", extra={'user_id': user_id})
        await callback.message.answer("⚠️ Выберите действие из меню", reply_markup=create_start_keyboard())
        await callback.answer()

# Handle country name input
@dp.message(ManageCountryStates.waiting_for_country_name)
async def process_country_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    country_name = message.text.strip().lower()
    user_logger.info(f"Entered country name: {country_name}", extra={'user_id': user_id})
    
    if not country_name.isalnum() and not all(c.isalnum() or c == '_' for c in country_name):
        user_logger.info(f"Invalid country name: {country_name}", extra={'user_id': user_id})
        await message.answer(
            "⚠️ Название должно содержать только латинские буквы, цифры или '_'\n"
            "Введите корректное название:",
            reply_markup=create_start_keyboard()
        )
        return
    
    await state.update_data(country_name=country_name)
    await message.answer(
        "🔑 Введите API ID (получить на https://my.telegram.org):",
        reply_markup=create_start_keyboard()
    )
    await state.set_state(ManageCountryStates.waiting_for_api_id)

# Handle API ID input
@dp.message(ManageCountryStates.waiting_for_api_id)
async def process_api_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    api_id = message.text.strip()
    user_logger.info(f"Entered API ID", extra={'user_id': user_id})
    
    if not api_id.isdigit():
        user_logger.info(f"Invalid API ID: {api_id}", extra={'user_id': user_id})
        await message.answer("⚠️ API ID — только цифры", reply_markup=create_start_keyboard())
        return
    
    await state.update_data(api_id=api_id)
    await message.answer(
        "🔑 Введите API Hash (получить на https://my.telegram.org):",
        reply_markup=create_start_keyboard()
    )
    await state.set_state(ManageCountryStates.waiting_for_api_hash)

# Handle API Hash input
@dp.message(ManageCountryStates.waiting_for_api_hash)
async def process_api_hash(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_logger.info(f"Entered API Hash", extra={'user_id': user_id})
    
    api_hash = message.text.strip()
    await state.update_data(api_hash=api_hash)
    await message.answer(
        "📱 Введите номер телефона (например, +79123456789):",
        reply_markup=create_start_keyboard()
    )
    await state.set_state(ManageCountryStates.waiting_for_phone_number)

# Handle phone number input
@dp.message(ManageCountryStates.waiting_for_phone_number)
async def process_phone_number(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    phone_number = message.text.strip()
    user_logger.info(f"Entered phone number", extra={'user_id': user_id})
    
    if not phone_number.startswith("+") or not phone_number[1:].isdigit():
        user_logger.info(f"Invalid phone number: {phone_number}", extra={'user_id': user_id})
        await message.answer("⚠️ Номер телефона должен начинаться с '+' и содержать только цифры", reply_markup=create_start_keyboard())
        return
    
    user_data = await state.get_data()
    country_name = user_data.get('country_name')
    api_id = user_data.get('api_id')
    api_hash = user_data.get('api_hash')
    
    if not add_country_config(country_name, api_id, api_hash, phone_number):
        user_logger.info(f"Failed to add country: {country_name}", extra={'user_id': user_id})
        await message.answer("⚠️ Ошибка при добавлении страны", reply_markup=create_start_keyboard())
        await state.clear()
        await handle_global_commands(message, state)
        return
    
    user_logger.info(f"Added country: {country_name}", extra={'user_id': user_id})
    
    country_config = {
        'country_name': country_name,
        'api_id': api_id,
        'api_hash': api_hash,
        'phone_number': phone_number,
        'session_name': f"{country_name}_session"
    }
    
    result = await create_new_client(country_config)
    if result.get('error'):
        user_logger.info(f"Failed to create client for {country_name}: {result['error']}", extra={'user_id': user_id})
        await message.answer(f"⚠️ Ошибка создания клиента: {result['error']}", reply_markup=create_start_keyboard())
        await state.clear()
        await handle_global_commands(message, state)
        return
    
    if result['needs_code']:
        await state.update_data(client=result['client'])
        await message.answer(
            "🔒 Введите код верификации, отправленный на ваш номер:",
            reply_markup=create_start_keyboard()
        )
        await state.set_state(ManageCountryStates.waiting_for_verification_code)
    else:
        await message.answer(
            f"✅ Страна {get_country_display_name(country_name)} успешно добавлена и авторизована",
            reply_markup=create_start_keyboard()
        )
        await state.clear()
        await handle_global_commands(message, state)

# Handle verification code
@dp.message(ManageCountryStates.waiting_for_verification_code)
async def process_verification_code(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    code = message.text.strip()
    user_logger.info(f"Entered verification code", extra={'user_id': user_id})
    
    user_data = await state.get_data()
    client = user_data.get('client')
    country_name = user_data.get('country_name')
    
    try:
        await client.sign_in(phone=user_data.get('country_config', {}).get('phone_number'), code=code)
        active_clients[country_name] = client
        await message.answer(
            f"✅ Страна {get_country_display_name(country_name)} успешно авторизована",
            reply_markup=create_start_keyboard()
        )
        user_logger.info(f"Authorized client for {country_name}", extra={'user_id': user_id})
    except Exception as e:
        user_logger.info(f"Authorization failed for {country_name}: {str(e)}", extra={'user_id': user_id})
        await message.answer(f"⚠️ Ошибка авторизации: {str(e)}", reply_markup=create_start_keyboard())
    
    await state.clear()
    await handle_global_commands(message, state)

# Handle country removal
@dp.callback_query(ManageCountryStates.waiting_for_country_to_remove)
async def process_country_to_remove(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    callback_data = callback.data
    user_logger.info(f"Selected country to remove: {callback_data}", extra={'user_id': user_id})
    
    if callback_data == "back_to_actions":
        buttons = [
            [InlineKeyboardButton(text="➕ Добавить страну", callback_data="action_add_country")],
            [InlineKeyboardButton(text="❌ Удалить страну", callback_data="action_remove_country")],
            [InlineKeyboardButton(text="<- Назад", callback_data="action_back")]
        ]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.delete()
        await callback.message.answer(
            "⚙️ <b>Управление странами</b>\n\n"
            "Выберите действие для настройки системы мониторинга:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.set_state(ManageCountryStates.waiting_for_action)
        await callback.answer()
        return
    
    if callback_data.startswith("remove_"):
        country_name = callback_data[len("remove_"):]
        if remove_country_config(country_name):
            if country_name in active_clients:
                await active_clients[country_name].disconnect()
                del active_clients[country_name]
            await callback.message.delete()
            await callback.message.answer(
                f"🗑️ Страна {get_country_display_name(country_name)} успешно удалена",
                reply_markup=create_start_keyboard()
            )
            user_logger.info(f"Removed country: {country_name}", extra={'user_id': user_id})
        else:
            await callback.message.delete()
            await callback.message.answer(
                f"⚠️ Страна {get_country_display_name(country_name)} не найдена",
                reply_markup=create_start_keyboard()
            )
        await state.clear()
        await handle_global_commands(callback.message, state)
        await callback.answer()
    else:
        user_logger.info(f"Invalid callback data: {callback_data}", extra={'user_id': user_id})
        await callback.message.answer("⚠️ Выберите страну из списка", reply_markup=create_start_keyboard())
        await callback.answer()

# Handle admin actions (add/remove user)
@dp.callback_query(lambda c: c.data in ["add_user", "remove_user"])
async def process_admin_action(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    callback_data = callback.data
    user_logger.info(f"Selected admin action: {callback_data}", extra={'user_id': user_id})
    
    if callback_data == "add_user":
        await callback.message.delete()
        await callback.message.answer("👤 Введите ID пользователя для добавления:", reply_markup=create_start_keyboard())
        await state.set_state(AdminStates.waiting_for_user_id)
        await callback.answer()
    
    elif callback_data == "remove_user":
        users = get_users()
        if not users:
            user_logger.info(f"No users to remove", extra={'user_id': user_id})
            await callback.message.delete()
            await callback.message.answer("⚠️ Нет пользователей для удаления", reply_markup=create_start_keyboard())
            await handle_global_commands(callback.message, state)
            await callback.answer()
            return
        
        buttons = []
        for uid in users:
            buttons.append([InlineKeyboardButton(
                text=f"👤 {uid}",
                callback_data=f"remove_user_{uid}"
            )])
        buttons.append([InlineKeyboardButton(text="◶ Назад", callback_data="back_to_admin")])
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.delete()
        await callback.message.answer(
            "🗑️ <b>Удаление пользователя</b>\n\nВыберите пользователя:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await state.set_state(AdminStates.waiting_for_user_to_remove)
        await callback.answer()

# Handle user ID input for adding
@dp.message(AdminStates.waiting_for_user_id)
async def process_user_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    new_user_id = message.text.strip()
    user_logger.info(f"Entered user ID to add: {new_user_id}", extra={'user_id': user_id})
    
    if not new_user_id.isdigit():
        user_logger.info(f"Invalid user ID: {new_user_id}", extra={'user_id': user_id})
        await message.answer("⚠️ ID пользователя должен быть числом", reply_markup=create_start_keyboard())
        return
    
    new_user_id = int(new_user_id)
    if add_user(new_user_id):
        await message.answer(f"✅ Пользователь {new_user_id} добавлен", reply_markup=create_start_keyboard())
    else:
        await message.answer(f"⚠️ Пользователь {new_user_id} уже существует или произошла ошибка", reply_markup=create_start_keyboard())
    
    await state.clear()
    await handle_global_commands(message, state)

# Handle user removal selection
@dp.callback_query(AdminStates.waiting_for_user_to_remove)
async def process_user_to_remove(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    callback_data = callback.data
    user_logger.info(f"Selected user to remove: {callback_data}", extra={'user_id': user_id})
    
    if callback_data == "back_to_admin":
        users = get_users()
        user_list = "\n".join([f"👤 {uid}" for uid in users]) if users else "Нет пользователей"
        
        buttons = [
            [InlineKeyboardButton(text="➕ Добавить пользователя", callback_data="add_user")],
            [InlineKeyboardButton(text="❌ Удалить пользователя", callback_data="remove_user")]
        ]
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
        await callback.message.delete()
        await callback.message.answer(
            f"📋 <b>Список пользователей:</b>\n{user_list}\n\nВыберите действие:",
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback.answer()
        return
    
    if callback_data.startswith("remove_user_"):
        user_to_remove = int(callback_data[len("remove_user_"):])
        if remove_user(user_to_remove):
            await callback.message.delete()
            await callback.message.answer(
                f"✅ Пользователь {user_to_remove} удален",
                reply_markup=create_start_keyboard()
            )
            user_logger.info(f"Removed user: {user_to_remove}", extra={'user_id': user_id})
        else:
            await callback.message.delete()
            await callback.message.answer(
                f"⚠️ Пользователь {user_to_remove} не найден",
                reply_markup=create_start_keyboard()
            )
            user_logger.info(f"User {user_to_remove} not found for removal", extra={'user_id': user_id})
        
        await state.clear()
        await handle_global_commands(callback.message, state)
        await callback.answer()
    else:
        user_logger.info(f"Invalid callback data: {callback_data}", extra={'user_id': user_id})
        await callback.message.answer("⚠️ Выберите пользователя из списка", reply_markup=create_start_keyboard())
        await callback.answer()

@dp.message()
async def handle_unexpected_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    user_logger.info(f"Received unexpected message: {message.text}", extra={'user_id': user_id})
    
    if message.text == "🌎 Список стран":
        countries = get_all_countries()
        if not countries:
            user_logger.info(f"No countries available", extra={'user_id': user_id})
            await message.answer("⚠️ Нет стран", reply_markup=create_start_keyboard())
            return
        
        await state.clear()
        await show_countries_list_inline(message)
        await state.set_state(ExportStates.waiting_for_country)
        return
    
    await message.answer(
        "⚠️ Используйте кнопку 'Список стран' или команды: /start, /settings, /log, /log_users, /dump, /admin",
        reply_markup=create_start_keyboard()
    )

# Bot startup
async def on_startup():
    try:
        logging.info("Starting bot...")
        init_db()
        
        countries = get_all_countries()
        for country in countries:
            result = await create_new_client(country)
            if result.get('error'):
                logging.error(f"Failed to start client for {country['country_name']}: {result['error']}")
            elif result['needs_code']:
                logging.warning(f"Client for {country['country_name']} requires manual authorization")
            else:
                logging.info(f"Client for {country['country_name']} started successfully")
        
        asyncio.create_task(check_clients())
        logging.info("Bot started successfully")
    except Exception as e:
        logging.error(f"Startup error: {e}")
        logging.error(traceback.format_exc())
        raise

# Main function to run the bot
async def main():
    try:
        await on_startup()
        await dp.start_polling(bot)
    except Exception as e:
        logging.error(f"Main loop error: {e}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        logging.error(traceback.format_exc())