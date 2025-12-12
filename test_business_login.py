import os
import logging
import sys
import time
from src.config.settings import Settings
from src.drivers.selenium_driver import SeleniumDriver
from src.parsers.gis_parser import GisParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/business_login_test.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# Ensure stdout/stderr encoding for Windows PowerShell
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
        sys.stderr.reconfigure(encoding='utf-8', errors='replace', line_buffering=True)
    except AttributeError:
        pass

def run_test_login():
    logger.info("=" * 80)
    logger.info("Starting 2GIS business account login test...")
    logger.info("=" * 80)

    # Load settings
    settings = Settings()
    
    if not hasattr(settings.parser, 'gis_business') or not settings.parser.gis_business.enabled:
        logger.warning("2GIS business account is not enabled in settings.")
        logger.info("Please set 2GIS_BUSINESS_ENABLED=true in .env or config.json")
        return False

    email = settings.parser.gis_business.email
    password = settings.parser.gis_business.password

    if not email or not password:
        logger.error("2GIS business account credentials (email/password) are not set.")
        logger.info("Please add 2GIS_BUSINESS_EMAIL and 2GIS_BUSINESS_PASSWORD to your .env file.")
        return False

    logger.info(f"Attempting login with email: {email}")
    logger.info(f"Password length: {len(password)} characters")

    driver = None
    try:
        logger.info("Initializing Selenium driver...")
        driver = SeleniumDriver(settings)
        driver.start()
        logger.info("Selenium driver started successfully")
        
        logger.info("Creating GisParser instance...")
        gis_parser = GisParser(driver, settings)
        logger.info("GisParser created successfully")

        logger.info("Calling _login_to_business_account method...")
        start_time = time.time()
        
        if gis_parser._login_to_business_account(email, password):
            elapsed_time = time.time() - start_time
            logger.info("=" * 80)
            logger.info("SUCCESS: Successfully logged into 2GIS business account!")
            logger.info(f"Login took {elapsed_time:.2f} seconds")
            logger.info("=" * 80)
            
            # Get current URL to verify
            current_url = driver.get_current_url()
            logger.info(f"Current URL after login: {current_url}")
            
            # Wait a bit to see if we stay logged in
            logger.info("Waiting 5 seconds to verify session...")
            time.sleep(5)
            
            final_url = driver.get_current_url()
            logger.info(f"Final URL: {final_url}")
            
            return True
        else:
            elapsed_time = time.time() - start_time
            logger.error("=" * 80)
            logger.error("FAILED: Could not login to 2GIS business account.")
            logger.error(f"Login attempt took {elapsed_time:.2f} seconds")
            logger.error("=" * 80)
            
            # Get current URL to see where we ended up
            current_url = driver.get_current_url()
            logger.error(f"Current URL: {current_url}")
            
            # Check if there are any debug HTML files saved
            debug_dir = os.path.join("debug", "2gis_login")
            if os.path.exists(debug_dir):
                debug_files = [f for f in os.listdir(debug_dir) if f.endswith('.html')]
                if debug_files:
                    logger.info(f"Debug HTML files saved in {debug_dir}:")
                    for f in sorted(debug_files)[-5:]:  # Show last 5 files
                        logger.info(f"  - {f}")
            
            return False
            
    except Exception as e:
        logger.error("=" * 80)
        logger.error(f"ERROR: An exception occurred during the login test: {e}", exc_info=True)
        logger.error("=" * 80)
        return False
    finally:
        if driver:
            logger.info("Stopping Selenium driver...")
            try:
                driver.stop()
                logger.info("Selenium driver stopped")
            except Exception as e:
                logger.warning(f"Error stopping driver: {e}")
        
        logger.info("=" * 80)
        logger.info("2GIS business login test finished.")
        logger.info("=" * 80)

if __name__ == "__main__":
    success = run_test_login()
    sys.exit(0 if success else 1)
