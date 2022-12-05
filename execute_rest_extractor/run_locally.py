from dotenv import load_dotenv

from execute_rest_extractor.handler import handle


# Import Environment Variables
load_dotenv()

if __name__ == "__main__":
    data = {"frontfill_lookback_min": "120"}
    handle(data)
