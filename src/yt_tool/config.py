import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv


load_dotenv()


@dataclass
class Config:
    youtube_api_key: str
    data_dir: Path = Path("data")
    batch_size: int = 5
    azure_api_key: str | None = None
    azure_endpoint: str | None = None
    azure_deployment: str | None = None
    azure_api_version: str | None = None


def load_config() -> Config:
    return Config(
        youtube_api_key=os.getenv("YOUTUBE_API_KEY", ""),
        data_dir=Path(os.getenv("YT_TOOL_DATA_DIR", "data")),
        batch_size=int(os.getenv("YT_TOOL_BATCH_SIZE", "5")),
        azure_api_key=os.getenv("AZURE_OPENAI_KEY"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        azure_deployment=os.getenv("AZURE_OPENAI_DEPLOYMENT"),
        azure_api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
    )

