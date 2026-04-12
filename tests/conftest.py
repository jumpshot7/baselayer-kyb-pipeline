import sys
from pathlib import Path
from dotenv import load_dotenv

# Tell Python to look inside backend/ for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))

# Load .env from project root
ENV_PATH = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)
