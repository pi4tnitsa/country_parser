
# Telegram Posts Monitoring Bot

This is a Telegram bot designed to monitor posts from Telegram channels across different countries, store them in a PostgreSQL database, and allow authorized users to export the collected data in Excel or JSON format. The bot supports multi-country configurations, user management, and logging for both bot operations and user actions.

## Features

- **Real-time Monitoring**: Monitors Telegram channels for new posts using Telethon.
- **Multi-country Support**: Configures separate Telegram clients for different countries.
- **Data Storage**: Stores channel posts in a PostgreSQL database with country-specific tables.
- **Data Export**: Exports posts for selected countries and periods in Excel or JSON format.
- **User Management**: Supports adding/removing authorized users via admin commands.
- **Country Management**: Allows adding/removing country configurations with Telegram API credentials.
- **Logging**: Maintains detailed logs for bot operations (`bot.log`) and user actions (`user_actions.log`).
- **Error Handling**: Robust error handling for database operations, Telegram API interactions, and user inputs.
- **Security**: Password-protected access to sensitive commands (`/settings`, `/log`, `/log_users`, `/dump`, `/admin`).

## Requirements

- Python 3.8+
- PostgreSQL database
- Telegram API credentials (API ID, API Hash, phone number) for each country
- Telegram Bot Token (obtained via BotFather)

### Python Dependencies

Listed in `requirements.txt`:

```text
aiogram==3.1.1
telethon>=1.28.5
psycopg2-binary>=2.9.5
pandas>=1.5.0
openpyxl>=3.1.0
pytz>=2023.3
```

Install dependencies using:

```bash
pip install -r requirements.txt
```

## Setup

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/yourusername/telegram-posts-monitoring-bot.git
   cd telegram-posts-monitoring-bot
   ```

2. **Configure Environment**:

   Create a `config.py` file based on `config.py.example`:

   ```python
   # config.py

   # Bot configuration
   BOT_TOKEN = 'your_bot_token_here'
   PASSWORD = 'your_secure_password_here'

   # Database configuration
   DB_NAME = 'your_database_name'
   DB_USER = 'your_database_user'
   DB_PASSWORD = 'your_database_password'  # Optional
   DB_HOST = 'your_database_host'  # e.g., 'localhost'
   DB_PORT = 'your_database_port'  # e.g., '5432'
   ```

   - **BOT_TOKEN**: Obtain from Telegram's BotFather.
   - **PASSWORD**: Set a secure password for admin commands.
   - **Database Settings**: Configure to match your PostgreSQL setup.

3. **Set Up PostgreSQL Database**:

   Ensure PostgreSQL is running and create a database:

   ```sql
   CREATE DATABASE your_database_name;
   ```

   The bot will automatically initialize the required tables (`channels`, `country_configs`, `users`, and country-specific `posts_*` tables) on startup.

4. **Obtain Telegram API Credentials**:

   - Go to [my.telegram.org](https://my.telegram.org).
   - Log in with a phone number and create an app to get `api_id` and `api_hash`.
   - These are required for each country configuration.

5. **Run the Bot**:

   ```bash
   python bot.py
   ```

   The bot will initialize the database, start Telegram clients for configured countries, and begin polling for commands.

## Usage

### Commands

- **/start**: Displays a list of configured countries and initiates the post export process.
- **/settings**: Password-protected command to manage country configurations (add/remove countries).
- **/log**: Password-protected command to download the bot's operation log (`bot.log`).
- **/log_users**: Password-protected command to download the user actions log (`user_actions.log`).
- **/dump**: Password-protected command to export all posts as a JSON file.
- **/admin**: Password-protected command to manage authorized users (add/remove users).

### Workflow

1. **Export Posts**:
   - Use `/start` to select a country from the inline keyboard.
   - Choose a time period (e.g., today, yesterday, custom month).
   - Select export format (Excel or JSON).
   - Receive a file with the posts for the selected country and period.

2. **Manage Countries**:
   - Use `/settings` and enter the password.
   - Add a new country by providing its name, API ID, API Hash, and phone number.
   - Authorize the country by entering the verification code sent by Telegram.
   - Remove a country to stop monitoring and archive its data.

3. **Manage Users**:
   - Use `/admin` and enter the password.
   - Add a new user by providing their Telegram user ID.
   - Remove an existing user from the authorized list.

4. **View Logs**:
   - Use `/log` or `/log_users` with the password to download logs.
   - Logs are trimmed to a maximum of 10,000 lines to prevent excessive growth.

### Database Structure

- **channels**: Stores channel metadata (ID, name, country).
- **country_configs**: Stores Telegram API credentials and session names for each country.
- **users**: Stores authorized user IDs.
- **posts_[country_name]**: Stores posts for each country (date, channel ID, channel name, content).

When a country is removed, its posts table is renamed to a backup table (e.g., `posts_[country]_backup_[timestamp]`) to preserve data.

## Logging

- **bot.log**: Contains detailed bot operations, including database interactions, Telegram API calls, and errors.
- **user_actions.log**: Records user commands and interactions for audit purposes.
- Logs are trimmed to 10,000 lines to manage file size.

## Error Handling

- **Database Errors**: Logged with stack traces; bot attempts to reconnect or notify the user.
- **Telegram API Errors**: Handles flood waits, authorization issues, and connection failures.
- **User Input Errors**: Validates inputs (e.g., country names, phone numbers) and prompts for corrections.
- **Client Monitoring**: Periodically checks Telegram client connections and restarts them if needed.

## Security Notes

- **Password Protection**: Sensitive commands require a password defined in `config.py`.
- **User Authorization**: Only users in the `users` table can access the bot's functionality.
- **Data Privacy**: Telegram API credentials and phone numbers are stored in the database; ensure proper database security.
- **Session Files**: Telethon session files are created per country (`[country_name]_session`); protect these files to prevent unauthorized access.

## Limitations

- Requires manual authorization (entering verification codes) when adding new countries.
- Custom month selection is limited to the current year.
- Media messages are stored as "medi-message (without text)" if they lack text content.
- Flood wait errors from Telegram may temporarily pause monitoring for a country.
